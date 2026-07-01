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

#include "folly/Benchmark.h"
#include "folly/init/Init.h"
#include "velox/dwio/common/FileSink.h"
#include "velox/dwio/parquet/writer/Writer.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/FlatVector.h"

using namespace facebook::velox;
using namespace facebook::velox::dwio::common;
using namespace facebook::velox::parquet;

namespace {

constexpr vector_size_t kNumRows = 100'000;
constexpr int kNumIterations = 50;
constexpr int kSinkSize = 200 * 1024 * 1024;

/// Writes a RowVector to a Parquet in-memory sink kNumIterations times.
/// Setup (vector creation, sink allocation) is excluded from timing.
void writeParquet(const RowVectorPtr& data, memory::MemoryPool* rootPool) {
  auto leafPool = rootPool->addLeafChild("sink");
  for (int i = 0; i < kNumIterations; ++i) {
    auto sink = std::make_unique<MemorySink>(
        kSinkSize, FileSink::Options{.pool = leafPool.get()});
    WriterOptions options;
    options.memoryPool = rootPool;
    options.formatSpecificOptions = std::make_shared<ParquetWriterOptions>();
    auto writer = std::make_unique<parquet::Writer>(
        std::move(sink), options, asRowType(data->type()));
    writer->write(data);
    writer->close();
  }
}

/// Builds a dictionary-encoded VARCHAR column with the given cardinality.
VectorPtr
makeDictVarchar(vector_size_t numRows, int dictSize, memory::MemoryPool* pool) {
  // Build stable string storage for the dictionary values.
  auto strings = std::make_shared<std::vector<std::string>>(dictSize);
  for (int i = 0; i < dictSize; ++i) {
    (*strings)[i] = fmt::format("value_{:06d}", i);
  }

  auto dictionary = BaseVector::create(VARCHAR(), dictSize, pool);
  auto* flat = dictionary->asFlatVector<StringView>();
  for (int i = 0; i < dictSize; ++i) {
    flat->set(i, StringView((*strings)[i]));
  }

  BufferPtr indices = AlignedBuffer::allocate<vector_size_t>(numRows, pool);
  auto rawIndices = indices->asMutable<vector_size_t>();
  for (vector_size_t i = 0; i < numRows; ++i) {
    rawIndices[i] = i % dictSize;
  }

  return BaseVector::wrapInDictionary(
      BufferPtr(nullptr), indices, numRows, dictionary);
}

/// Builds a dictionary-encoded INTEGER column with the given cardinality.
VectorPtr
makeDictInteger(vector_size_t numRows, int dictSize, memory::MemoryPool* pool) {
  auto dictionary = BaseVector::create(INTEGER(), dictSize, pool);
  auto* flat = dictionary->asFlatVector<int32_t>();
  for (int i = 0; i < dictSize; ++i) {
    flat->set(i, i * 7);
  }

  BufferPtr indices = AlignedBuffer::allocate<vector_size_t>(numRows, pool);
  auto rawIndices = indices->asMutable<vector_size_t>();
  for (vector_size_t i = 0; i < numRows; ++i) {
    rawIndices[i] = i % dictSize;
  }

  return BaseVector::wrapInDictionary(
      BufferPtr(nullptr), indices, numRows, dictionary);
}

/// Builds a flat VARCHAR column (control case, no dictionary).
VectorPtr makeFlatVarchar(vector_size_t numRows, memory::MemoryPool* pool) {
  auto vector = BaseVector::create(VARCHAR(), numRows, pool);
  auto* flat = vector->asFlatVector<StringView>();
  auto strings = std::make_shared<std::vector<std::string>>(numRows);
  for (vector_size_t i = 0; i < numRows; ++i) {
    (*strings)[i] = fmt::format("value_{:06d}", i % 10);
  }
  for (vector_size_t i = 0; i < numRows; ++i) {
    flat->set(i, StringView((*strings)[i]));
  }
  return vector;
}

/// Builds a flat INTEGER column (control case).
VectorPtr makeFlatInteger(vector_size_t numRows, memory::MemoryPool* pool) {
  auto vector = BaseVector::create(INTEGER(), numRows, pool);
  auto* flat = vector->asFlatVector<int32_t>();
  for (vector_size_t i = 0; i < numRows; ++i) {
    flat->set(i, static_cast<int32_t>(i));
  }
  return vector;
}

std::shared_ptr<memory::MemoryPool> rootPool;

// -- Dictionary VARCHAR benchmarks at various cardinalities --

void benchDictVarchar(int dictSize) {
  folly::BenchmarkSuspender suspender;
  auto leafPool = rootPool->addLeafChild("bench");
  auto column = makeDictVarchar(kNumRows, dictSize, leafPool.get());
  auto data = std::make_shared<RowVector>(
      leafPool.get(),
      ROW({"c0"}, {VARCHAR()}),
      BufferPtr(nullptr),
      kNumRows,
      std::vector<VectorPtr>{column});
  suspender.dismiss();
  writeParquet(data, rootPool.get());
}

BENCHMARK(DictVarchar_Card10) {
  benchDictVarchar(10);
}
BENCHMARK(DictVarchar_Card100) {
  benchDictVarchar(100);
}
BENCHMARK(DictVarchar_Card1000) {
  benchDictVarchar(1'000);
}
BENCHMARK(DictVarchar_Card10000) {
  benchDictVarchar(10'000);
}

BENCHMARK_DRAW_LINE();

// -- Dictionary INTEGER benchmarks --

void benchDictInteger(int dictSize) {
  folly::BenchmarkSuspender suspender;
  auto leafPool = rootPool->addLeafChild("bench");
  auto column = makeDictInteger(kNumRows, dictSize, leafPool.get());
  auto data = std::make_shared<RowVector>(
      leafPool.get(),
      ROW({"c0"}, {INTEGER()}),
      BufferPtr(nullptr),
      kNumRows,
      std::vector<VectorPtr>{column});
  suspender.dismiss();
  writeParquet(data, rootPool.get());
}

BENCHMARK(DictInteger_Card10) {
  benchDictInteger(10);
}
BENCHMARK(DictInteger_Card100) {
  benchDictInteger(100);
}
BENCHMARK(DictInteger_Card1000) {
  benchDictInteger(1'000);
}

BENCHMARK_DRAW_LINE();

// -- Flat baseline benchmarks (no dictionary, control case) --

BENCHMARK(FlatVarchar) {
  folly::BenchmarkSuspender suspender;
  auto leafPool = rootPool->addLeafChild("bench");
  auto column = makeFlatVarchar(kNumRows, leafPool.get());
  auto data = std::make_shared<RowVector>(
      leafPool.get(),
      ROW({"c0"}, {VARCHAR()}),
      BufferPtr(nullptr),
      kNumRows,
      std::vector<VectorPtr>{column});
  suspender.dismiss();
  writeParquet(data, rootPool.get());
}

BENCHMARK(FlatInteger) {
  folly::BenchmarkSuspender suspender;
  auto leafPool = rootPool->addLeafChild("bench");
  auto column = makeFlatInteger(kNumRows, leafPool.get());
  auto data = std::make_shared<RowVector>(
      leafPool.get(),
      ROW({"c0"}, {INTEGER()}),
      BufferPtr(nullptr),
      kNumRows,
      std::vector<VectorPtr>{column});
  suspender.dismiss();
  writeParquet(data, rootPool.get());
}

BENCHMARK_DRAW_LINE();

// -- Multi-column benchmarks for selective flattening --
// These test the case where one column forces flattening (dict-of-dict) while
// other columns are passthrough dictionaries.  With blanket flattening, all
// columns get materialized.  With selective flattening, only the one that
// needs it is flattened.

/// Builds a dict-of-dict INTEGER column (forces flattening in needFlatten).
VectorPtr makeDictOfDictInteger(
    vector_size_t numRows,
    int dictSize,
    memory::MemoryPool* pool) {
  auto dictionary = BaseVector::create(INTEGER(), dictSize, pool);
  auto* flat = dictionary->asFlatVector<int32_t>();
  for (int i = 0; i < dictSize; ++i) {
    flat->set(i, i * 13);
  }

  BufferPtr innerIdx = AlignedBuffer::allocate<vector_size_t>(numRows, pool);
  auto rawInner = innerIdx->asMutable<vector_size_t>();
  for (vector_size_t i = 0; i < numRows; ++i) {
    rawInner[i] = i % dictSize;
  }
  auto innerDict = BaseVector::wrapInDictionary(
      BufferPtr(nullptr), innerIdx, numRows, dictionary);

  BufferPtr outerIdx = AlignedBuffer::allocate<vector_size_t>(numRows, pool);
  auto rawOuter = outerIdx->asMutable<vector_size_t>();
  for (vector_size_t i = 0; i < numRows; ++i) {
    rawOuter[i] = i;
  }
  return BaseVector::wrapInDictionary(
      BufferPtr(nullptr), outerIdx, numRows, innerDict);
}

/// N passthrough dict VARCHAR columns + 1 dict-of-dict column that forces
/// flattening.  With blanket flattening all N+1 columns are flattened; with
/// selective flattening only the dict-of-dict column is.
void benchMixedColumns(int numPassthroughCols) {
  folly::BenchmarkSuspender suspender;
  auto leafPool = rootPool->addLeafChild("bench");

  std::vector<VectorPtr> columns;
  std::vector<std::string> names;
  std::vector<TypePtr> types;

  // Passthrough dict VARCHAR columns (low cardinality).
  for (int i = 0; i < numPassthroughCols; ++i) {
    columns.push_back(makeDictVarchar(kNumRows, 10, leafPool.get()));
    names.push_back(fmt::format("dict_{}", i));
    types.push_back(VARCHAR());
  }

  // One dict-of-dict INTEGER column that forces flattening.
  columns.push_back(makeDictOfDictInteger(kNumRows, 50, leafPool.get()));
  names.push_back("nested");
  types.push_back(INTEGER());

  auto data = std::make_shared<RowVector>(
      leafPool.get(),
      ROW(std::move(names), std::move(types)),
      BufferPtr(nullptr),
      kNumRows,
      std::move(columns));

  suspender.dismiss();
  writeParquet(data, rootPool.get());
}

BENCHMARK(Mixed_1DictPassthrough_1Nested) {
  benchMixedColumns(1);
}
BENCHMARK(Mixed_5DictPassthrough_1Nested) {
  benchMixedColumns(5);
}
BENCHMARK(Mixed_10DictPassthrough_1Nested) {
  benchMixedColumns(10);
}
BENCHMARK(Mixed_20DictPassthrough_1Nested) {
  benchMixedColumns(20);
}

BENCHMARK_DRAW_LINE();

/// Control: N passthrough dict VARCHAR columns with NO column that forces
/// flattening.  Should show no difference between blanket and selective.
void benchAllPassthroughColumns(int numCols) {
  folly::BenchmarkSuspender suspender;
  auto leafPool = rootPool->addLeafChild("bench");

  std::vector<VectorPtr> columns;
  std::vector<std::string> names;
  std::vector<TypePtr> types;

  for (int i = 0; i < numCols; ++i) {
    columns.push_back(makeDictVarchar(kNumRows, 10, leafPool.get()));
    names.push_back(fmt::format("c{}", i));
    types.push_back(VARCHAR());
  }

  auto data = std::make_shared<RowVector>(
      leafPool.get(),
      ROW(std::move(names), std::move(types)),
      BufferPtr(nullptr),
      kNumRows,
      std::move(columns));

  suspender.dismiss();
  writeParquet(data, rootPool.get());
}

BENCHMARK(AllPassthrough_5Cols) {
  benchAllPassthroughColumns(5);
}
BENCHMARK(AllPassthrough_10Cols) {
  benchAllPassthroughColumns(10);
}

BENCHMARK_DRAW_LINE();

// -- Null handling benchmarks --
// These measure the cost of the indices detach (null at wrapping layer) vs
// full flattening (null in dictionary values) vs no-null passthrough.

/// Dictionary with nulls at the WRAPPING layer (indices null bitmap set).
/// This follows the detach path: indices are copied, dictionary values shared.
VectorPtr makeDictVarcharWithNullIndices(
    vector_size_t numRows,
    int dictSize,
    double nullRatio,
    memory::MemoryPool* pool) {
  auto strings = std::make_shared<std::vector<std::string>>(dictSize);
  for (int i = 0; i < dictSize; ++i) {
    (*strings)[i] = fmt::format("value_{:06d}", i);
  }

  auto dictionary = BaseVector::create(VARCHAR(), dictSize, pool);
  auto* flat = dictionary->asFlatVector<StringView>();
  for (int i = 0; i < dictSize; ++i) {
    flat->set(i, StringView((*strings)[i]));
  }

  BufferPtr indices = AlignedBuffer::allocate<vector_size_t>(numRows, pool);
  auto rawIndices = indices->asMutable<vector_size_t>();
  for (vector_size_t i = 0; i < numRows; ++i) {
    rawIndices[i] = i % dictSize;
  }

  // Create null bitmap at the wrapping layer.
  BufferPtr nulls = AlignedBuffer::allocate<bool>(numRows, pool);
  auto rawNulls = nulls->asMutable<uint64_t>();
  for (vector_size_t i = 0; i < numRows; ++i) {
    bits::setNull(rawNulls, i, (i % 100) < static_cast<int>(nullRatio * 100));
  }

  return BaseVector::wrapInDictionary(nulls, indices, numRows, dictionary);
}

/// Dictionary whose VALUES array contains nulls. This triggers full flattening
/// because Arrow does not support DictionaryArray with nulls in the dictionary.
VectorPtr makeDictVarcharWithNullValues(
    vector_size_t numRows,
    int dictSize,
    double nullRatio,
    memory::MemoryPool* pool) {
  auto strings = std::make_shared<std::vector<std::string>>(dictSize);
  for (int i = 0; i < dictSize; ++i) {
    (*strings)[i] = fmt::format("value_{:06d}", i);
  }

  auto dictionary = BaseVector::create(VARCHAR(), dictSize, pool);
  auto* flat = dictionary->asFlatVector<StringView>();
  for (int i = 0; i < dictSize; ++i) {
    if ((i % static_cast<int>(1.0 / nullRatio)) == 0) {
      flat->setNull(i, true);
    } else {
      flat->set(i, StringView((*strings)[i]));
    }
  }

  BufferPtr indices = AlignedBuffer::allocate<vector_size_t>(numRows, pool);
  auto rawIndices = indices->asMutable<vector_size_t>();
  for (vector_size_t i = 0; i < numRows; ++i) {
    rawIndices[i] = i % dictSize;
  }

  return BaseVector::wrapInDictionary(
      BufferPtr(nullptr), indices, numRows, dictionary);
}

void benchDictVarcharNullIndices(int dictSize, double nullRatio) {
  folly::BenchmarkSuspender suspender;
  auto leafPool = rootPool->addLeafChild("bench");
  auto column =
      makeDictVarcharWithNullIndices(kNumRows, dictSize, nullRatio, leafPool.get());
  auto data = std::make_shared<RowVector>(
      leafPool.get(),
      ROW({"c0"}, {VARCHAR()}),
      BufferPtr(nullptr),
      kNumRows,
      std::vector<VectorPtr>{column});
  suspender.dismiss();
  writeParquet(data, rootPool.get());
}

void benchDictVarcharNullValues(int dictSize, double nullRatio) {
  folly::BenchmarkSuspender suspender;
  auto leafPool = rootPool->addLeafChild("bench");
  auto column =
      makeDictVarcharWithNullValues(kNumRows, dictSize, nullRatio, leafPool.get());
  auto data = std::make_shared<RowVector>(
      leafPool.get(),
      ROW({"c0"}, {VARCHAR()}),
      BufferPtr(nullptr),
      kNumRows,
      std::vector<VectorPtr>{column});
  suspender.dismiss();
  writeParquet(data, rootPool.get());
}

// Null at wrapping layer: passthrough + detach (cheap memcpy of indices).
BENCHMARK(DictVarchar_NullIndices_10pct_Card10) {
  benchDictVarcharNullIndices(10, 0.1);
}
BENCHMARK(DictVarchar_NullIndices_50pct_Card10) {
  benchDictVarcharNullIndices(10, 0.5);
}
BENCHMARK(DictVarchar_NullIndices_10pct_Card1000) {
  benchDictVarcharNullIndices(1'000, 0.1);
}
BENCHMARK(DictVarchar_NullIndices_50pct_Card1000) {
  benchDictVarcharNullIndices(1'000, 0.5);
}

BENCHMARK_DRAW_LINE();

// Null in dictionary values: forced full flatten (expensive materialization).
BENCHMARK(DictVarchar_NullValues_10pct_Card10) {
  benchDictVarcharNullValues(10, 0.1);
}
BENCHMARK(DictVarchar_NullValues_50pct_Card10) {
  benchDictVarcharNullValues(10, 0.5);
}
BENCHMARK(DictVarchar_NullValues_10pct_Card1000) {
  benchDictVarcharNullValues(1'000, 0.1);
}
BENCHMARK(DictVarchar_NullValues_50pct_Card1000) {
  benchDictVarcharNullValues(1'000, 0.5);
}

BENCHMARK_DRAW_LINE();

// -- Multi-batch with shared indices (FileDataSink pattern) --
// Measures the cost of indices detach across multiple write() calls where the
// same buffer is reused (simulating partitioned writes).

void benchMultiBatchSharedIndices(int numBatches, int dictSize) {
  folly::BenchmarkSuspender suspender;
  auto leafPool = rootPool->addLeafChild("bench");
  constexpr vector_size_t kBatchSize = 10'000;

  auto strings = std::make_shared<std::vector<std::string>>(dictSize);
  for (int i = 0; i < dictSize; ++i) {
    (*strings)[i] = fmt::format("value_{:06d}", i);
  }
  auto dictionary = BaseVector::create(VARCHAR(), dictSize, leafPool.get());
  auto* flat = dictionary->asFlatVector<StringView>();
  for (int i = 0; i < dictSize; ++i) {
    flat->set(i, StringView((*strings)[i]));
  }

  // Shared indices buffer, overwritten each batch (like partitionRows_).
  BufferPtr sharedIndices =
      AlignedBuffer::allocate<vector_size_t>(kBatchSize, leafPool.get());

  suspender.dismiss();

  auto sinkPool = rootPool->addLeafChild("sink");
  for (int iter = 0; iter < kNumIterations; ++iter) {
    auto sink = std::make_unique<MemorySink>(
        kSinkSize, FileSink::Options{.pool = sinkPool.get()});
    WriterOptions options;
    options.memoryPool = rootPool.get();
    options.formatSpecificOptions = std::make_shared<ParquetWriterOptions>();
    auto writer = std::make_unique<parquet::Writer>(
        std::move(sink), options, ROW({"c0"}, {VARCHAR()}));
    for (int batch = 0; batch < numBatches; ++batch) {
      auto rawIndices = sharedIndices->asMutable<vector_size_t>();
      for (vector_size_t i = 0; i < kBatchSize; ++i) {
        rawIndices[i] = (i + batch * 7) % dictSize;
      }
      auto dictCol = BaseVector::wrapInDictionary(
          BufferPtr(nullptr), sharedIndices, kBatchSize, dictionary);
      auto data = std::make_shared<RowVector>(
          leafPool.get(),
          ROW({"c0"}, {VARCHAR()}),
          BufferPtr(nullptr),
          kBatchSize,
          std::vector<VectorPtr>{dictCol});
      writer->write(data);
    }
    writer->close();
  }
}

BENCHMARK(MultiBatch_10x10K_Card10_SharedIdx) {
  benchMultiBatchSharedIndices(10, 10);
}
BENCHMARK(MultiBatch_50x10K_Card10_SharedIdx) {
  benchMultiBatchSharedIndices(50, 10);
}
BENCHMARK(MultiBatch_10x10K_Card1000_SharedIdx) {
  benchMultiBatchSharedIndices(10, 1'000);
}
BENCHMARK(MultiBatch_50x10K_Card1000_SharedIdx) {
  benchMultiBatchSharedIndices(50, 1'000);
}

BENCHMARK_DRAW_LINE();

// -- Partition routing simulation (exec::wrap pattern) --
// The dictionary IS the full original flat vector (with duplicates), and the
// indices select a subset of rows. This is what FileDataSink actually produces
// when partitioning.

void benchPartitionRouting(int numPartitions, int batchSize, int numUnique) {
  folly::BenchmarkSuspender suspender;
  auto leafPool = rootPool->addLeafChild("bench");

  // Build a flat vector representing the original batch (with duplicates).
  auto strings = std::make_shared<std::vector<std::string>>(batchSize);
  for (int i = 0; i < batchSize; ++i) {
    (*strings)[i] = fmt::format("value_{:06d}", i % numUnique);
  }
  auto fullBatch = BaseVector::create(VARCHAR(), batchSize, leafPool.get());
  auto* flat = fullBatch->asFlatVector<StringView>();
  for (int i = 0; i < batchSize; ++i) {
    flat->set(i, StringView((*strings)[i]));
  }

  // Each partition gets batchSize/numPartitions rows.
  const vector_size_t partitionSize = batchSize / numPartitions;
  BufferPtr sharedIndices =
      AlignedBuffer::allocate<vector_size_t>(partitionSize, leafPool.get());

  suspender.dismiss();

  auto sinkPool = rootPool->addLeafChild("sink");
  // Simulate writing all partitions from one batch, repeated.
  for (int iter = 0; iter < kNumIterations; ++iter) {
    for (int p = 0; p < numPartitions; ++p) {
      auto sink = std::make_unique<MemorySink>(
          kSinkSize, FileSink::Options{.pool = sinkPool.get()});
      WriterOptions options;
      options.memoryPool = rootPool.get();
      options.formatSpecificOptions = std::make_shared<ParquetWriterOptions>();
      auto writer = std::make_unique<parquet::Writer>(
          std::move(sink), options, ROW({"c0"}, {VARCHAR()}));

      // Fill indices selecting rows for this partition (strided pattern).
      auto rawIndices = sharedIndices->asMutable<vector_size_t>();
      for (vector_size_t i = 0; i < partitionSize; ++i) {
        rawIndices[i] = p + i * numPartitions;
      }
      auto dictCol = BaseVector::wrapInDictionary(
          BufferPtr(nullptr), sharedIndices, partitionSize, fullBatch);
      auto data = std::make_shared<RowVector>(
          leafPool.get(),
          ROW({"c0"}, {VARCHAR()}),
          BufferPtr(nullptr),
          partitionSize,
          std::vector<VectorPtr>{dictCol});
      writer->write(data);
      writer->close();
    }
  }
}

// 4 partitions from 10K batch, 10 unique values.
BENCHMARK(PartitionRouting_4part_10K_10uniq) {
  benchPartitionRouting(4, 10'000, 10);
}
// 4 partitions from 10K batch, 1000 unique values.
BENCHMARK(PartitionRouting_4part_10K_1000uniq) {
  benchPartitionRouting(4, 10'000, 1'000);
}
// 4 partitions from 100K batch, 10 unique values.
BENCHMARK(PartitionRouting_4part_100K_10uniq) {
  benchPartitionRouting(4, 100'000, 10);
}
// 4 partitions from 100K batch, 1000 unique values.
BENCHMARK(PartitionRouting_4part_100K_1000uniq) {
  benchPartitionRouting(4, 100'000, 1'000);
}

BENCHMARK_DRAW_LINE();

// -- Flush estimation benchmarks --

void benchFlushEstimation(int numBatches) {
  folly::BenchmarkSuspender suspender;
  auto leafPool = rootPool->addLeafChild("bench");
  constexpr vector_size_t kBatchSize = 10'000;
  constexpr int kDictSize = 10;

  auto dict = makeDictVarchar(kBatchSize, kDictSize, leafPool.get());
  auto batch = std::make_shared<RowVector>(
      leafPool.get(),
      ROW({"c0"}, {VARCHAR()}),
      BufferPtr(nullptr),
      kBatchSize,
      std::vector<VectorPtr>{dict});

  suspender.dismiss();

  auto sinkPool = rootPool->addLeafChild("sink");
  for (int iter = 0; iter < kNumIterations; ++iter) {
    auto sink = std::make_unique<MemorySink>(
        kSinkSize, FileSink::Options{.pool = sinkPool.get()});
    WriterOptions options;
    options.memoryPool = rootPool.get();
    options.flushPolicyFactory = []() {
      return std::make_unique<DefaultFlushPolicy>(
          /*rowsInRowGroup=*/1'000'000,
          /*bytesInRowGroup=*/512 * 1'024);
    };
    options.formatSpecificOptions = std::make_shared<ParquetWriterOptions>();
    auto writer = std::make_unique<parquet::Writer>(
        std::move(sink), options, asRowType(batch->type()));
    for (int i = 0; i < numBatches; ++i) {
      writer->write(batch);
    }
    writer->close();
  }
}

BENCHMARK(FlushEstimation_50Batches) {
  benchFlushEstimation(50);
}
BENCHMARK(FlushEstimation_200Batches) {
  benchFlushEstimation(200);
}

} // namespace

int32_t main(int32_t argc, char* argv[]) {
  folly::Init init{&argc, &argv};
  memory::MemoryManager::initialize(memory::MemoryManager::Options{});
  rootPool = memory::memoryManager()->addRootPool("ParquetWriterBenchmark");
  folly::runBenchmarks();
  return 0;
}
