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

// Parquet decoder benchmarks covering types and encodings not exercised by the
// existing ParquetReaderBenchmarkMain.cpp.  Each section targets a specific
// optimization opportunity identified in the decoding improvement plan.

#include "velox/dwio/parquet/tests/reader/ParquetReaderBenchmark.h"

using namespace facebook::velox;
using namespace facebook::velox::dwio;
using namespace facebook::velox::dwio::common;
using namespace facebook::velox::parquet;
using namespace facebook::velox::parquet::test;
using namespace facebook::velox::test;

// ---------------------------------------------------------------------------
// Helper: no-filter benchmark (select 100% of rows).
//
// For types that don't support range filters in the benchmark harness
// (BOOLEAN, TIMESTAMP) we always use selectPct=100 and only vary the null
// rate and batch size to isolate decoder performance.
// ---------------------------------------------------------------------------

#define PARQUET_BENCH_NO_FILTER_NULLS(_type_, _name_, _null_, _next_, _dict_) \
  BENCHMARK_NAMED_PARAM(                                                      \
      run,                                                                    \
      _name_##_Nulls_##_null_##_next_##_next_##_##_dict_,                     \
      #_name_,                                                                \
      _type_,                                                                 \
      100,                                                                    \
      _null_,                                                                 \
      _next_,                                                                 \
      _dict_);

// A focused macro: one batch size (10k), dict vs plain, varying null rates.
#define PARQUET_BENCH_TYPE_PLAIN_DICT(_type_, _name_)                      \
  PARQUET_BENCH_NO_FILTER_NULLS(_type_, _name_, 0, 10000, false)          \
  PARQUET_BENCH_NO_FILTER_NULLS(_type_, _name_, 0, 10000, true)           \
  PARQUET_BENCH_NO_FILTER_NULLS(_type_, _name_, 20, 10000, false)         \
  PARQUET_BENCH_NO_FILTER_NULLS(_type_, _name_, 20, 10000, true)          \
  PARQUET_BENCH_NO_FILTER_NULLS(_type_, _name_, 50, 10000, false)         \
  PARQUET_BENCH_NO_FILTER_NULLS(_type_, _name_, 50, 10000, true)          \
  BENCHMARK_DRAW_LINE();

// Batch-size sweep for a type (plain only, 0% nulls) to measure per-batch
// overhead.
#define PARQUET_BENCH_BATCH_SWEEP(_type_, _name_)                         \
  PARQUET_BENCH_NO_FILTER_NULLS(_type_, _name_, 0, 1000, true)            \
  PARQUET_BENCH_NO_FILTER_NULLS(_type_, _name_, 0, 5000, true)            \
  PARQUET_BENCH_NO_FILTER_NULLS(_type_, _name_, 0, 10000, true)           \
  PARQUET_BENCH_NO_FILTER_NULLS(_type_, _name_, 0, 50000, true)           \
  PARQUET_BENCH_NO_FILTER_NULLS(_type_, _name_, 0, 100000, true)          \
  BENCHMARK_DRAW_LINE();

// Macro for compressed benchmarks using the extended run() overload.
#define PARQUET_BENCH_COMPRESSED(_type_, _name_, _codec_)                  \
  BENCHMARK_NAMED_PARAM(                                                  \
      run,                                                                \
      _name_##_##_codec_##_Nulls_0_next_10k_plain,                        \
      #_name_,                                                            \
      _type_,                                                             \
      100,                                                                \
      0,                                                                  \
      10000,                                                              \
      true,                                                               \
      common::CompressionKind_##_codec_);

// ===========================================================================
// Section 1: TIMESTAMP decoding
//
// Measures: TimestampColumnReader (always slow path, hasBulkPath()=false).
// Baseline for optimization 1.2 (INT64 timestamp bulk path).
// ===========================================================================

PARQUET_BENCH_TYPE_PLAIN_DICT(TIMESTAMP(), Timestamp)
PARQUET_BENCH_BATCH_SWEEP(TIMESTAMP(), TimestampBatch)

// ===========================================================================
// Section 2: BOOLEAN decoding
//
// Measures: BooleanDecoder (PLAIN, bit-by-bit) and RleBpDataDecoder (RLE).
// Baseline for optimization 2.2 (batch boolean decoding).
// The Parquet writer always writes PLAIN booleans; RLE booleans would need
// specially crafted files. We benchmark what the writer produces.
// ===========================================================================

PARQUET_BENCH_TYPE_PLAIN_DICT(BOOLEAN(), Boolean)
PARQUET_BENCH_BATCH_SWEEP(BOOLEAN(), BooleanBatch)

// ===========================================================================
// Section 3: INTEGER (INT32) -- type widening baseline
//
// Measures: DirectDecoder PLAIN path with INT32 physical type.
// When read into Velox BIGINT, the readContiguous loop does per-element
// widening. Baseline for optimization 2.1 (SIMD type widening).
// ===========================================================================

PARQUET_BENCH_TYPE_PLAIN_DICT(INTEGER(), Integer)
PARQUET_BENCH_BATCH_SWEEP(INTEGER(), IntegerBatch)

// ===========================================================================
// Section 4: SMALLINT and TINYINT -- narrower INT32 widening
//
// The Parquet writer stores SMALLINT/TINYINT as INT32 physical type. Reading
// them exercises the same widening/narrowing paths.
// ===========================================================================

PARQUET_BENCH_TYPE_PLAIN_DICT(SMALLINT(), SmallInt)
PARQUET_BENCH_TYPE_PLAIN_DICT(TINYINT(), TinyInt)

// ===========================================================================
// Section 5: REAL (FLOAT) -- float decoding baseline
// ===========================================================================

PARQUET_BENCH_TYPE_PLAIN_DICT(REAL(), Real)

// ===========================================================================
// Section 6: Compression codec comparison
//
// Uses BIGINT PLAIN with different compression codecs to isolate
// decompression overhead per page. Baseline for optimization 2.5
// (LZ4/GZIP direct decompression).
// ===========================================================================

PARQUET_BENCH_COMPRESSED(BIGINT(), BigIntCompr, SNAPPY)
PARQUET_BENCH_COMPRESSED(BIGINT(), BigIntCompr, ZSTD)
PARQUET_BENCH_COMPRESSED(BIGINT(), BigIntCompr, GZIP)
PARQUET_BENCH_COMPRESSED(BIGINT(), BigIntCompr, LZ4)
PARQUET_BENCH_COMPRESSED(BIGINT(), BigIntCompr, NONE)
BENCHMARK_DRAW_LINE();

// Same for VARCHAR to measure string + compression interaction.
PARQUET_BENCH_COMPRESSED(VARCHAR(), VarcharCompr, SNAPPY)
PARQUET_BENCH_COMPRESSED(VARCHAR(), VarcharCompr, ZSTD)
PARQUET_BENCH_COMPRESSED(VARCHAR(), VarcharCompr, GZIP)
PARQUET_BENCH_COMPRESSED(VARCHAR(), VarcharCompr, LZ4)
PARQUET_BENCH_COMPRESSED(VARCHAR(), VarcharCompr, NONE)
BENCHMARK_DRAW_LINE();

// ===========================================================================
// Section 7: Short Decimal PLAIN vs DICT
//
// Measures: IntegerColumnReader with short decimal.
// PLAIN short decimals lack a bulk path (hasBulkPath()=false unless dict).
// Baseline for optimization 1.3.
// ===========================================================================

// Note: already in existing benchmarks, but we add a batch sweep for
// isolating per-batch overhead with plain encoding.
PARQUET_BENCH_BATCH_SWEEP(DECIMAL(18, 3), ShortDecimalBatch)

// ===========================================================================
// Section 8: DATE -- maps to INT32 internally
// ===========================================================================

PARQUET_BENCH_TYPE_PLAIN_DICT(DATE(), Date)

// ===========================================================================
// Main
// ===========================================================================

int main(int argc, char** argv) {
  folly::Init init{&argc, &argv};
  memory::MemoryManager::initialize(memory::MemoryManager::Options{});
  folly::runBenchmarks();
  return 0;
}
