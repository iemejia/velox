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

#pragma once

#include <atomic>
#include <cstdint>

namespace facebook::velox::parquet::arrow {

/// Collects optional diagnostic counters for the Parquet writer benchmark.
/// The benchmark runs one instrumented writer at a time, while field tasks may
/// update these counters concurrently.
struct WriterBenchmarkStats {
  std::atomic<int64_t> veloxExportNanos{0};
  std::atomic<int64_t> schemaAndImportNanos{0};
  std::atomic<int64_t> arrowWriterOpenNanos{0};
  std::atomic<int64_t> recordBatchWriteNanos{0};
  std::atomic<int64_t> postWriteAccountingNanos{0};
  std::atomic<int64_t> fieldWriterSetupNanos{0};
  std::atomic<int64_t> taskSubmitAndWaitNanos{0};
  std::atomic<int64_t> fieldTaskWallNanos{0};
  std::atomic<int64_t> fieldTaskCount{0};
  std::atomic<int64_t> rowGroupCloseNanos{0};
  std::atomic<int64_t> columnCloseNanos{0};
  std::atomic<int64_t> compressionNanos{0};
  std::atomic<int64_t> compressionInputBytes{0};
  std::atomic<int64_t> compressionOutputBytes{0};
  std::atomic<int64_t> finalColumnCopyNanos{0};
  std::atomic<int64_t> finalColumnCopyBytes{0};
};

} // namespace facebook::velox::parquet::arrow
