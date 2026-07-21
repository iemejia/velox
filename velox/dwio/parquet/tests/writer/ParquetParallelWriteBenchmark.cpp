/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <time.h>
#include <chrono>
#include <iostream>
#include <optional>
#include <random>
#include <string>
#include <vector>

#include <folly/Benchmark.h>
#include <folly/init/Init.h>
#include <folly/json.h>
#include <gflags/gflags.h>

#include <arrow/c/bridge.h>
#include <arrow/io/memory.h>
#include <arrow/record_batch.h>
#include <arrow/util/thread_pool.h>

#include "velox/dwio/common/FileSink.h"
#include "velox/dwio/parquet/writer/Writer.h"
#include "velox/dwio/parquet/writer/arrow/BenchmarkStats.h"
#include "velox/dwio/parquet/writer/arrow/Platform.h"
#include "velox/dwio/parquet/writer/arrow/Properties.h"
#include "velox/dwio/parquet/writer/arrow/Writer.h"
#include "velox/vector/tests/utils/VectorMaker.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

DEFINE_string(
    mode,
    "cold",
    "Lifecycle: cold, warm, arrow_cold, or arrow_warm. Arrow modes export the "
    "input before timing.");
DEFINE_string(
    workload,
    "flat_compressible",
    "Input: bigint, dictionary, flat_compressible, or flat_random.");
DEFINE_string(codec, "zstd", "Compression codec: none, snappy, or zstd.");
DEFINE_int32(workers, 1, "Number of column-write worker threads.");
DEFINE_bool(
    threaded_one_worker,
    false,
    "Use the threaded Arrow path with one worker to measure fixed overhead.");
DEFINE_bool(
    deferred_serial,
    false,
    "Construct all field writers before executing them serially.");
DEFINE_bool(
    per_field_contexts,
    false,
    "Give deferred serial field writers independent ArrowWriteContexts.");
DEFINE_int32(columns, 20, "Number of top-level columns.");
DEFINE_int32(rows, 100000, "Rows per input batch.");
DEFINE_int32(batches, 8, "Batches written by warm mode.");
DEFINE_int32(trials, 10, "Measured trials emitted as individual JSON lines.");
DEFINE_int32(warmups, 2, "Sacrificial trials before measurement.");
DEFINE_int32(seed, 42, "Seed used by deterministic random-string inputs.");
DEFINE_bool(
    instrument,
    false,
    "Collect internal phase counters. Disable for confirmatory timing.");
DEFINE_bool(
    hash_output,
    false,
    "Hash output bytes after timing. Use for serial/parallel equivalence checks.");
DEFINE_bool(
    verify_output,
    false,
    "Write serial and threaded variants and compare complete output bytes.");

using namespace facebook::velox;
using namespace facebook::velox::dwio::common;
using namespace facebook::velox::parquet;

namespace {

using Clock = std::chrono::steady_clock;
using WriterBenchmarkStats =
    facebook::velox::parquet::arrow::WriterBenchmarkStats;
using ArrowFileWriter = facebook::velox::parquet::arrow::arrow::FileWriter;

int64_t elapsedNanos(Clock::time_point start) {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(
             Clock::now() - start)
      .count();
}

int64_t processCpuNanos() {
  timespec time{};
  VELOX_CHECK_EQ(clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &time), 0);
  return time.tv_sec * 1'000'000'000LL + time.tv_nsec;
}

// Discards output after counting bytes. Hash validation uses a MemorySink in a
// separate run so it cannot affect confirmatory timing.
class CountingSink final : public FileSink {
 public:
  explicit CountingSink(memory::MemoryPool* pool)
      : FileSink("CountingSink", FileSink::Options{.pool = pool}) {}

  ~CountingSink() override {
    markClosed();
  }

  using FileSink::write;

  void write(std::vector<DataBuffer<char>>& buffers) override {
    for (const auto& buffer : buffers) {
      size_ += buffer.size();
    }
  }
};

uint64_t hashBytes(const char* data, uint64_t size) {
  uint64_t hash = 1'469'598'103'934'665'603ULL;
  for (uint64_t index = 0; index < size; ++index) {
    hash ^= static_cast<uint8_t>(data[index]);
    hash *= 1'099'511'628'211ULL;
  }
  return hash;
}

struct SinkHandle {
  std::unique_ptr<FileSink> sink;
  CountingSink* counting{nullptr};
  MemorySink* memory{nullptr};
};

SinkHandle makeSink(memory::MemoryPool* pool) {
  if (FLAGS_hash_output) {
    auto sink =
        std::make_unique<MemorySink>(0, FileSink::Options{.pool = pool});
    auto* sinkPtr = sink.get();
    return SinkHandle{.sink = std::move(sink), .memory = sinkPtr};
  }
  auto sink = std::make_unique<CountingSink>(pool);
  auto* sinkPtr = sink.get();
  return SinkHandle{.sink = std::move(sink), .counting = sinkPtr};
}

uint64_t sinkSize(const SinkHandle& handle) {
  return handle.memory ? handle.memory->size() : handle.counting->size();
}

std::optional<uint64_t> sinkHash(const SinkHandle& handle) {
  if (!handle.memory) {
    return std::nullopt;
  }
  return hashBytes(handle.memory->data(), handle.memory->size());
}

struct StatsSnapshot {
  int64_t veloxExportNanos;
  int64_t schemaAndImportNanos;
  int64_t arrowWriterOpenNanos;
  int64_t recordBatchWriteNanos;
  int64_t postWriteAccountingNanos;
  int64_t fieldWriterSetupNanos;
  int64_t taskSubmitAndWaitNanos;
  int64_t fieldTaskWallNanos;
  int64_t fieldTaskCount;
  int64_t rowGroupCloseNanos;
  int64_t columnCloseNanos;
  int64_t compressionNanos;
  int64_t compressionInputBytes;
  int64_t compressionOutputBytes;
  int64_t finalColumnCopyNanos;
  int64_t finalColumnCopyBytes;
};

StatsSnapshot snapshot(const WriterBenchmarkStats& stats) {
  return StatsSnapshot{
      .veloxExportNanos = stats.veloxExportNanos.load(),
      .schemaAndImportNanos = stats.schemaAndImportNanos.load(),
      .arrowWriterOpenNanos = stats.arrowWriterOpenNanos.load(),
      .recordBatchWriteNanos = stats.recordBatchWriteNanos.load(),
      .postWriteAccountingNanos = stats.postWriteAccountingNanos.load(),
      .fieldWriterSetupNanos = stats.fieldWriterSetupNanos.load(),
      .taskSubmitAndWaitNanos = stats.taskSubmitAndWaitNanos.load(),
      .fieldTaskWallNanos = stats.fieldTaskWallNanos.load(),
      .fieldTaskCount = stats.fieldTaskCount.load(),
      .rowGroupCloseNanos = stats.rowGroupCloseNanos.load(),
      .columnCloseNanos = stats.columnCloseNanos.load(),
      .compressionNanos = stats.compressionNanos.load(),
      .compressionInputBytes = stats.compressionInputBytes.load(),
      .compressionOutputBytes = stats.compressionOutputBytes.load(),
      .finalColumnCopyNanos = stats.finalColumnCopyNanos.load(),
      .finalColumnCopyBytes = stats.finalColumnCopyBytes.load(),
  };
}

StatsSnapshot subtract(const StatsSnapshot& end, const StatsSnapshot& start) {
  return StatsSnapshot{
      .veloxExportNanos = end.veloxExportNanos - start.veloxExportNanos,
      .schemaAndImportNanos =
          end.schemaAndImportNanos - start.schemaAndImportNanos,
      .arrowWriterOpenNanos =
          end.arrowWriterOpenNanos - start.arrowWriterOpenNanos,
      .recordBatchWriteNanos =
          end.recordBatchWriteNanos - start.recordBatchWriteNanos,
      .postWriteAccountingNanos =
          end.postWriteAccountingNanos - start.postWriteAccountingNanos,
      .fieldWriterSetupNanos =
          end.fieldWriterSetupNanos - start.fieldWriterSetupNanos,
      .taskSubmitAndWaitNanos =
          end.taskSubmitAndWaitNanos - start.taskSubmitAndWaitNanos,
      .fieldTaskWallNanos = end.fieldTaskWallNanos - start.fieldTaskWallNanos,
      .fieldTaskCount = end.fieldTaskCount - start.fieldTaskCount,
      .rowGroupCloseNanos = end.rowGroupCloseNanos - start.rowGroupCloseNanos,
      .columnCloseNanos = end.columnCloseNanos - start.columnCloseNanos,
      .compressionNanos = end.compressionNanos - start.compressionNanos,
      .compressionInputBytes =
          end.compressionInputBytes - start.compressionInputBytes,
      .compressionOutputBytes =
          end.compressionOutputBytes - start.compressionOutputBytes,
      .finalColumnCopyNanos =
          end.finalColumnCopyNanos - start.finalColumnCopyNanos,
      .finalColumnCopyBytes =
          end.finalColumnCopyBytes - start.finalColumnCopyBytes,
  };
}

folly::dynamic statsJson(const StatsSnapshot& stats) {
  return folly::dynamic::object("velox_export_ns", stats.veloxExportNanos)(
      "schema_import_ns", stats.schemaAndImportNanos)(
      "arrow_writer_open_ns", stats.arrowWriterOpenNanos)(
      "record_batch_write_ns", stats.recordBatchWriteNanos)(
      "post_write_accounting_ns", stats.postWriteAccountingNanos)(
      "field_writer_setup_ns", stats.fieldWriterSetupNanos)(
      "task_submit_wait_ns", stats.taskSubmitAndWaitNanos)(
      "field_task_wall_sum_ns", stats.fieldTaskWallNanos)(
      "field_task_count", stats.fieldTaskCount)(
      "row_group_close_ns", stats.rowGroupCloseNanos)(
      "column_close_ns", stats.columnCloseNanos)(
      "compression_ns", stats.compressionNanos)(
      "compression_input_bytes", stats.compressionInputBytes)(
      "compression_output_bytes", stats.compressionOutputBytes)(
      "final_column_copy_ns", stats.finalColumnCopyNanos)(
      "final_column_copy_bytes", stats.finalColumnCopyBytes);
}

common::CompressionKind compressionKind() {
  if (FLAGS_codec == "none") {
    return common::CompressionKind_NONE;
  }
  if (FLAGS_codec == "snappy") {
    return common::CompressionKind_SNAPPY;
  }
  if (FLAGS_codec == "zstd") {
    return common::CompressionKind_ZSTD;
  }
  VELOX_USER_FAIL("Unsupported codec: {}", FLAGS_codec);
}

facebook::velox::parquet::arrow::Compression::type arrowCompression() {
  if (FLAGS_codec == "none") {
    return facebook::velox::parquet::arrow::Compression::UNCOMPRESSED;
  }
  if (FLAGS_codec == "snappy") {
    return facebook::velox::parquet::arrow::Compression::SNAPPY;
  }
  if (FLAGS_codec == "zstd") {
    return facebook::velox::parquet::arrow::Compression::ZSTD;
  }
  VELOX_USER_FAIL("Unsupported codec: {}", FLAGS_codec);
}

std::string randomString(std::mt19937& generator, int32_t length) {
  std::uniform_int_distribution<int32_t> distribution(32, 126);
  std::string value(length, '\0');
  for (char& character : value) {
    character = static_cast<char>(distribution(generator));
  }
  return value;
}

VectorPtr makeColumn(
    const std::string& workload,
    int32_t column,
    vector_size_t numRows,
    memory::MemoryPool* pool) {
  test::VectorMaker maker(pool);
  if (workload == "bigint") {
    return maker.flatVector<int64_t>(numRows, [column](vector_size_t row) {
      return static_cast<int64_t>(row) * 1'000'003 + column;
    });
  }
  if (workload == "dictionary") {
    constexpr int32_t kCardinality = 100;
    auto dictionary = maker.flatVector<std::string>(
        kCardinality, [column](vector_size_t index) {
          return fmt::format("column_{:02d}_value_{:04d}", column, index);
        });
    auto indices = test::makeIndices(
        numRows, [](vector_size_t row) { return row % kCardinality; }, pool);
    return BaseVector::wrapInDictionary(
        BufferPtr(nullptr), indices, numRows, dictionary);
  }
  if (workload == "flat_compressible") {
    return maker.flatVector<std::string>(numRows, [column](vector_size_t row) {
      auto prefix = fmt::format("column={:02d};row={:08d};", column, row);
      std::string value(128, 'a' + (column % 26));
      value.replace(0, prefix.size(), prefix);
      return value;
    });
  }
  if (workload == "flat_random") {
    std::mt19937 generator(FLAGS_seed + column * 1'000'003);
    std::vector<std::string> values;
    values.reserve(numRows);
    for (vector_size_t row = 0; row < numRows; ++row) {
      values.push_back(randomString(generator, 128));
    }
    return maker.flatVector<std::string>(values);
  }
  VELOX_USER_FAIL("Unsupported workload: {}", workload);
}

RowVectorPtr makeInput(memory::MemoryPool* pool) {
  test::VectorMaker maker(pool);
  std::vector<std::string> names;
  std::vector<VectorPtr> columns;
  names.reserve(FLAGS_columns);
  columns.reserve(FLAGS_columns);
  for (int32_t column = 0; column < FLAGS_columns; ++column) {
    names.push_back(fmt::format("c{}", column));
    columns.push_back(makeColumn(FLAGS_workload, column, FLAGS_rows, pool));
  }
  return maker.rowVector(std::move(names), std::move(columns));
}

WriterOptions makeWriterOptions(memory::MemoryPool* pool) {
  WriterOptions options;
  options.memoryPool = pool;
  options.compressionKind = compressionKind();
  auto parquetOptions = std::make_shared<ParquetWriterOptions>();
  parquetOptions->columnWriteParallelism = FLAGS_workers;
  parquetOptions->useThreadedColumnWritePath = FLAGS_threaded_one_worker;
  parquetOptions->useDeferredSerialColumnWritePath = FLAGS_deferred_serial;
  parquetOptions->usePerFieldWriteContexts = FLAGS_per_field_contexts;
  parquetOptions->enableDictionary = FLAGS_workload == "dictionary";
  options.formatSpecificOptions = std::move(parquetOptions);
  return options;
}

std::shared_ptr<::arrow::RecordBatch> makeRecordBatch(
    const RowVectorPtr& input,
    memory::MemoryPool* pool) {
  ArrowOptions options{.flattenDictionary = true, .flattenConstant = true};
  ArrowArray array;
  exportToArrow(input, array, pool, options);
  ArrowSchema schema;
  exportToArrow(input, schema, options);
  auto arrowSchema = ::arrow::ImportSchema(&schema).ValueOrDie();
  return ::arrow::ImportRecordBatch(&array, arrowSchema).ValueOrDie();
}

std::shared_ptr<facebook::velox::parquet::arrow::WriterProperties>
makeArrowWriterProperties(
    const std::shared_ptr<WriterBenchmarkStats>& benchmarkStats) {
  facebook::velox::parquet::arrow::WriterProperties::Builder builder;
  builder.benchmarkStats(benchmarkStats);
  builder.compression(arrowCompression());
  if (FLAGS_workload == "dictionary") {
    builder.enableDictionary();
  } else {
    builder.disableDictionary();
  }
  return builder.build();
}

struct ArrowExecution {
  std::shared_ptr<::arrow::internal::ThreadPool> executor;
  std::shared_ptr<facebook::velox::parquet::arrow::ArrowWriterProperties>
      properties;
};

ArrowExecution makeArrowExecution() {
  facebook::velox::parquet::arrow::ArrowWriterProperties::Builder builder;
  builder.setDeferColumnWrites(FLAGS_deferred_serial);
  builder.setUsePerFieldWriteContexts(FLAGS_per_field_contexts);
  std::shared_ptr<::arrow::internal::ThreadPool> executor;
  if (FLAGS_workers > 1 || FLAGS_threaded_one_worker) {
    PARQUET_ASSIGN_OR_THROW(
        executor, ::arrow::internal::ThreadPool::Make(FLAGS_workers));
    builder.setUseThreads(true);
    builder.setExecutor(executor.get());
  }
  return ArrowExecution{.executor = executor, .properties = builder.build()};
}

folly::dynamic baseJson(int32_t trial) {
  return folly::dynamic::object("trial", trial)("mode", FLAGS_mode)(
      "workload", FLAGS_workload)("codec", FLAGS_codec)(
      "workers", FLAGS_workers)(
      "threaded_one_worker", FLAGS_threaded_one_worker)(
      "deferred_serial", FLAGS_deferred_serial)(
      "per_field_contexts", FLAGS_per_field_contexts)("columns", FLAGS_columns)(
      "rows", FLAGS_rows)("batches", FLAGS_batches)(
      "instrumented", FLAGS_instrument)("hash_output", FLAGS_hash_output);
}

folly::dynamic runCold(
    int32_t trial,
    const RowVectorPtr& input,
    memory::MemoryPool* rootPool) {
  WriterBenchmarkStats internalStats;
  auto benchmarkStats = FLAGS_instrument
      ? std::make_shared<WriterBenchmarkStats>()
      : std::shared_ptr<WriterBenchmarkStats>{};

  const auto totalStart = Clock::now();
  const auto totalCpuStart = processCpuNanos();
  auto sinkPool = rootPool->addLeafChild(fmt::format("cold_sink_{}", trial));
  auto sinkHandle = makeSink(sinkPool.get());
  auto options = makeWriterOptions(rootPool);
  std::static_pointer_cast<ParquetWriterOptions>(options.formatSpecificOptions)
      ->benchmarkStats = benchmarkStats;
  auto writerPool =
      rootPool->addAggregateChild(fmt::format("cold_writer_{}", trial));

  const auto constructStart = Clock::now();
  auto writer = std::make_unique<parquet::Writer>(
      std::move(sinkHandle.sink),
      options,
      writerPool,
      asRowType(input->type()));
  const auto constructNanos = elapsedNanos(constructStart);

  const auto writeStartStats =
      benchmarkStats ? snapshot(*benchmarkStats) : StatsSnapshot{};
  const auto writeStart = Clock::now();
  const auto writeCpuStart = processCpuNanos();
  writer->write(input);
  const auto writeNanos = elapsedNanos(writeStart);
  const auto writeCpuNanos = processCpuNanos() - writeCpuStart;
  const auto afterWriteStats =
      benchmarkStats ? snapshot(*benchmarkStats) : StatsSnapshot{};

  const auto closeStart = Clock::now();
  writer->close();
  const auto closeNanos = elapsedNanos(closeStart);
  const auto afterCloseStats =
      benchmarkStats ? snapshot(*benchmarkStats) : StatsSnapshot{};
  const auto outputBytes = sinkSize(sinkHandle);
  const auto elapsedBeforeValidation = elapsedNanos(totalStart);
  const auto outputHash = sinkHash(sinkHandle);

  const auto destroyStart = Clock::now();
  writer.reset();
  writerPool.reset();
  sinkPool.reset();
  const auto destroyNanos = elapsedNanos(destroyStart);
  const auto totalNanos = elapsedBeforeValidation + destroyNanos;
  const auto totalCpuNanos = processCpuNanos() - totalCpuStart;

  auto result = baseJson(trial);
  result["construct_ns"] = constructNanos;
  result["write_ns"] = writeNanos;
  result["write_cpu_ns"] = writeCpuNanos;
  result["close_ns"] = closeNanos;
  result["destroy_ns"] = destroyNanos;
  result["total_ns"] = totalNanos;
  result["total_cpu_ns"] = totalCpuNanos;
  result["output_bytes"] = static_cast<int64_t>(outputBytes);
  if (outputHash) {
    result["output_hash"] = fmt::format("{:016x}", *outputHash);
    folly::doNotOptimizeAway(*outputHash);
  }
  if (FLAGS_instrument) {
    result["write_stats"] =
        statsJson(subtract(afterWriteStats, writeStartStats));
    result["close_stats"] =
        statsJson(subtract(afterCloseStats, afterWriteStats));
  }
  folly::doNotOptimizeAway(outputBytes);
  return result;
}

folly::dynamic runWarm(
    int32_t trial,
    const RowVectorPtr& input,
    memory::MemoryPool* rootPool) {
  VELOX_USER_CHECK_GT(
      FLAGS_batches, 1, "Warm mode requires at least 2 batches");
  WriterBenchmarkStats internalStats;
  auto benchmarkStats = FLAGS_instrument
      ? std::make_shared<WriterBenchmarkStats>()
      : std::shared_ptr<WriterBenchmarkStats>{};

  auto sinkPool = rootPool->addLeafChild(fmt::format("warm_sink_{}", trial));
  auto sinkHandle = makeSink(sinkPool.get());
  auto options = makeWriterOptions(rootPool);
  std::static_pointer_cast<ParquetWriterOptions>(options.formatSpecificOptions)
      ->benchmarkStats = benchmarkStats;
  auto writerPool =
      rootPool->addAggregateChild(fmt::format("warm_writer_{}", trial));
  auto writer = std::make_unique<parquet::Writer>(
      std::move(sinkHandle.sink),
      options,
      writerPool,
      asRowType(input->type()));

  const auto firstStart = Clock::now();
  writer->write(input);
  writer->flush();
  const auto firstBatchNanos = elapsedNanos(firstStart);

  const auto steadyStartStats =
      benchmarkStats ? snapshot(*benchmarkStats) : StatsSnapshot{};
  const auto steadyStart = Clock::now();
  const auto steadyCpuStart = processCpuNanos();
  for (int32_t batch = 1; batch < FLAGS_batches; ++batch) {
    writer->write(input);
    writer->flush();
  }
  const auto steadyNanos = elapsedNanos(steadyStart);
  const auto steadyCpuNanos = processCpuNanos() - steadyCpuStart;
  const auto steadyEndStats =
      benchmarkStats ? snapshot(*benchmarkStats) : StatsSnapshot{};

  const auto closeStart = Clock::now();
  writer->close();
  const auto closeNanos = elapsedNanos(closeStart);
  const auto outputBytes = sinkSize(sinkHandle);
  const auto outputHash = sinkHash(sinkHandle);
  writer.reset();
  writerPool.reset();
  sinkPool.reset();

  auto result = baseJson(trial);
  result["first_batch_ns"] = firstBatchNanos;
  result["steady_batches_ns"] = steadyNanos;
  result["steady_batches_cpu_ns"] = steadyCpuNanos;
  result["steady_batch_avg_ns"] = steadyNanos / (FLAGS_batches - 1);
  result["steady_batch_cpu_avg_ns"] = steadyCpuNanos / (FLAGS_batches - 1);
  result["close_ns"] = closeNanos;
  result["output_bytes"] = static_cast<int64_t>(outputBytes);
  if (outputHash) {
    result["output_hash"] = fmt::format("{:016x}", *outputHash);
    folly::doNotOptimizeAway(*outputHash);
  }
  if (FLAGS_instrument) {
    result["steady_stats"] =
        statsJson(subtract(steadyEndStats, steadyStartStats));
  }
  folly::doNotOptimizeAway(outputBytes);
  return result;
}

folly::dynamic runArrow(
    int32_t trial,
    const std::shared_ptr<::arrow::RecordBatch>& recordBatch,
    bool warm) {
  WriterBenchmarkStats internalStats;
  auto benchmarkStats = FLAGS_instrument
      ? std::make_shared<WriterBenchmarkStats>()
      : std::shared_ptr<WriterBenchmarkStats>{};

  const auto totalStart = Clock::now();
  const auto totalCpuStart = processCpuNanos();
  auto output = facebook::velox::parquet::arrow::createOutputStream();
  std::shared_ptr<::arrow::io::OutputStream> outputStream = output;
  auto execution = makeArrowExecution();
  const auto constructStart = Clock::now();
  PARQUET_ASSIGN_OR_THROW(
      auto writer,
      ArrowFileWriter::open(
          *recordBatch->schema(),
          ::arrow::default_memory_pool(),
          outputStream,
          makeArrowWriterProperties(benchmarkStats),
          execution.properties));
  const auto constructNanos = elapsedNanos(constructStart);

  const auto firstStart = Clock::now();
  const auto firstCpuStart = processCpuNanos();
  PARQUET_THROW_NOT_OK(writer->writeRecordBatch(*recordBatch));
  if (warm) {
    PARQUET_THROW_NOT_OK(writer->finishRowGroup());
  }
  const auto firstNanos = elapsedNanos(firstStart);
  const auto firstCpuNanos = processCpuNanos() - firstCpuStart;

  int64_t steadyNanos{0};
  int64_t steadyCpuNanos{0};
  StatsSnapshot steadyStats{};
  if (warm) {
    const auto steadyStartStats =
        benchmarkStats ? snapshot(*benchmarkStats) : StatsSnapshot{};
    const auto steadyStart = Clock::now();
    const auto steadyCpuStart = processCpuNanos();
    for (int32_t batch = 1; batch < FLAGS_batches; ++batch) {
      PARQUET_THROW_NOT_OK(writer->writeRecordBatch(*recordBatch));
      PARQUET_THROW_NOT_OK(writer->finishRowGroup());
    }
    steadyNanos = elapsedNanos(steadyStart);
    steadyCpuNanos = processCpuNanos() - steadyCpuStart;
    steadyStats = subtract(
        benchmarkStats ? snapshot(*benchmarkStats) : StatsSnapshot{},
        steadyStartStats);
  }

  const auto closeStart = Clock::now();
  PARQUET_THROW_NOT_OK(writer->close());
  const auto closeNanos = elapsedNanos(closeStart);
  PARQUET_ASSIGN_OR_THROW(const auto outputBytes, output->Tell());
  const auto destroyStart = Clock::now();
  writer.reset();
  execution.executor.reset();
  const auto destroyNanos = elapsedNanos(destroyStart);
  const auto totalNanos = elapsedNanos(totalStart);
  const auto totalCpuNanos = processCpuNanos() - totalCpuStart;
  std::optional<uint64_t> outputHash;
  if (FLAGS_hash_output) {
    PARQUET_ASSIGN_OR_THROW(auto buffer, output->Finish());
    outputHash = hashBytes(
        reinterpret_cast<const char*>(buffer->data()), buffer->size());
  }

  auto result = baseJson(trial);
  result["construct_ns"] = constructNanos;
  result["first_batch_ns"] = firstNanos;
  result["first_batch_cpu_ns"] = firstCpuNanos;
  result["close_ns"] = closeNanos;
  result["destroy_ns"] = destroyNanos;
  result["total_ns"] = totalNanos;
  result["total_cpu_ns"] = totalCpuNanos;
  result["output_bytes"] = outputBytes;
  if (outputHash) {
    result["output_hash"] = fmt::format("{:016x}", *outputHash);
    folly::doNotOptimizeAway(*outputHash);
  }
  if (warm) {
    result["steady_batches_ns"] = steadyNanos;
    result["steady_batches_cpu_ns"] = steadyCpuNanos;
    result["steady_batch_avg_ns"] = steadyNanos / (FLAGS_batches - 1);
    result["steady_batch_cpu_avg_ns"] = steadyCpuNanos / (FLAGS_batches - 1);
    if (FLAGS_instrument) {
      result["steady_stats"] = statsJson(steadyStats);
    }
  }
  folly::doNotOptimizeAway(outputBytes);
  return result;
}

std::shared_ptr<::arrow::Buffer> writeArrowFileForVerification(
    const std::shared_ptr<::arrow::RecordBatch>& recordBatch,
    int32_t workers,
    bool threaded) {
  auto output = facebook::velox::parquet::arrow::createOutputStream();
  std::shared_ptr<::arrow::io::OutputStream> outputStream = output;
  facebook::velox::parquet::arrow::ArrowWriterProperties::Builder arrowBuilder;
  std::shared_ptr<::arrow::internal::ThreadPool> executor;
  if (threaded) {
    PARQUET_ASSIGN_OR_THROW(
        executor, ::arrow::internal::ThreadPool::Make(workers));
    arrowBuilder.setUseThreads(true);
    arrowBuilder.setExecutor(executor.get());
  }
  PARQUET_ASSIGN_OR_THROW(
      auto writer,
      ArrowFileWriter::open(
          *recordBatch->schema(),
          ::arrow::default_memory_pool(),
          outputStream,
          makeArrowWriterProperties(nullptr),
          arrowBuilder.build()));
  PARQUET_THROW_NOT_OK(writer->writeRecordBatch(*recordBatch));
  PARQUET_THROW_NOT_OK(writer->close());
  writer.reset();
  executor.reset();
  PARQUET_ASSIGN_OR_THROW(auto buffer, output->Finish());
  return buffer;
}

void verifyOutputs(const std::shared_ptr<::arrow::RecordBatch>& recordBatch) {
  const auto serial = writeArrowFileForVerification(recordBatch, 1, false);
  folly::dynamic variants = folly::dynamic::array;
  for (const int32_t workers : {1, 2, 4}) {
    const auto threaded =
        writeArrowFileForVerification(recordBatch, workers, true);
    VELOX_CHECK_EQ(serial->size(), threaded->size());
    VELOX_CHECK_EQ(
        std::memcmp(serial->data(), threaded->data(), serial->size()), 0);
    variants.push_back(
        folly::dynamic::object("workers", workers)("bytes", serial->size())(
            "identical", true));
  }
  std::cout << folly::toJson(
                   folly::dynamic::object("verification", "byte_for_byte")(
                       "variants", variants))
            << '\n';
}

void validateFlags() {
  VELOX_USER_CHECK(
      FLAGS_mode == "cold" || FLAGS_mode == "warm" ||
          FLAGS_mode == "arrow_cold" || FLAGS_mode == "arrow_warm",
      "Unsupported mode: {}",
      FLAGS_mode);
  VELOX_USER_CHECK_GT(FLAGS_workers, 0, "workers must be positive");
  VELOX_USER_CHECK_GT(FLAGS_columns, 0, "columns must be positive");
  VELOX_USER_CHECK_GT(FLAGS_rows, 0, "rows must be positive");
  VELOX_USER_CHECK_GE(FLAGS_trials, 1, "trials must be at least 1");
  VELOX_USER_CHECK_GE(FLAGS_warmups, 0, "warmups must be non-negative");
  VELOX_USER_CHECK(
      !FLAGS_threaded_one_worker || FLAGS_workers == 1,
      "threaded_one_worker requires workers=1");
  VELOX_USER_CHECK(
      !FLAGS_deferred_serial || FLAGS_workers == 1,
      "deferred_serial requires workers=1");
  VELOX_USER_CHECK(
      !FLAGS_deferred_serial || !FLAGS_threaded_one_worker,
      "deferred_serial and threaded_one_worker are mutually exclusive");
  VELOX_USER_CHECK(
      !FLAGS_per_field_contexts || FLAGS_deferred_serial,
      "per_field_contexts requires deferred_serial");
  compressionKind();
}

} // namespace

int32_t main(int32_t argc, char** argv) {
  folly::Init init{&argc, &argv};
  validateFlags();
  memory::MemoryManager::initialize(memory::MemoryManager::Options{});
  auto rootPool =
      memory::memoryManager()->addRootPool("ParquetParallelWriteBenchmark");
  auto inputPool = rootPool->addLeafChild("input");
  auto input = makeInput(inputPool.get());
  const bool arrowMode =
      FLAGS_mode == "arrow_cold" || FLAGS_mode == "arrow_warm";
  auto recordBatch = arrowMode ? makeRecordBatch(input, inputPool.get())
                               : std::shared_ptr<::arrow::RecordBatch>{};
  if (FLAGS_verify_output) {
    VELOX_USER_CHECK(arrowMode, "verify_output requires an Arrow mode");
    verifyOutputs(recordBatch);
    return 0;
  }

  for (int32_t warmup = 0; warmup < FLAGS_warmups; ++warmup) {
    if (FLAGS_mode == "arrow_cold" || FLAGS_mode == "arrow_warm") {
      runArrow(-1, recordBatch, FLAGS_mode == "arrow_warm");
    } else if (FLAGS_mode == "cold") {
      runCold(-1, input, rootPool.get());
    } else {
      runWarm(-1, input, rootPool.get());
    }
  }
  for (int32_t trial = 0; trial < FLAGS_trials; ++trial) {
    folly::dynamic result;
    if (FLAGS_mode == "arrow_cold" || FLAGS_mode == "arrow_warm") {
      result = runArrow(trial, recordBatch, FLAGS_mode == "arrow_warm");
    } else {
      result = FLAGS_mode == "cold" ? runCold(trial, input, rootPool.get())
                                    : runWarm(trial, input, rootPool.get());
    }
    std::cout << folly::toJson(result) << '\n';
  }
  return 0;
}
