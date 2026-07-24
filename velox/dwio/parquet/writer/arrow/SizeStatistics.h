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

// Adapted from Apache Arrow.

#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <vector>

#include "velox/dwio/parquet/writer/arrow/Platform.h"

namespace facebook::velox::parquet::arrow {

class ColumnDescriptor;

/// Captures metadata for estimating the unencoded, uncompressed size of data
/// written. Useful for readers to estimate reconstruction memory and for
/// fine-grained filter push down on nested structures.
struct PARQUET_EXPORT SizeStatistics {
  /// One element per definition level (size = max definition level + 1), each
  /// counting how many times that definition level was observed. Empty when
  /// max definition level is 0.
  std::vector<int64_t> definitionLevelHistogram;

  /// Same as definitionLevelHistogram but for repetition levels. Empty when
  /// max repetition level is 0.
  std::vector<int64_t> repetitionLevelHistogram;

  /// Number of physical bytes stored for BYTE_ARRAY values assuming no
  /// encoding, excluding the per-value length prefix. Only set for BYTE_ARRAY
  /// columns.
  std::optional<int64_t> unencodedByteArrayDataBytes;

  /// Whether any statistics are populated.
  bool isSet() const {
    return !repetitionLevelHistogram.empty() ||
        !definitionLevelHistogram.empty() ||
        unencodedByteArrayDataBytes.has_value();
  }

  /// Increments the unencoded BYTE_ARRAY data byte count.
  void incrementUnencodedByteArrayDataBytes(int64_t value);

  /// Merges another SizeStatistics into this one. Throws if incompatible.
  void merge(const SizeStatistics& other);

  /// Validates the histograms against the column descriptor. Throws on
  /// mismatch or if unencoded bytes are set for a non-BYTE_ARRAY column.
  void validate(const ColumnDescriptor* descr) const;

  /// Resets all populated statistics to zero, keeping their shape.
  void reset();

  /// Makes an empty SizeStatistics sized for the given column.
  static std::unique_ptr<SizeStatistics> make(const ColumnDescriptor* descr);
};

/// Accumulates the counts of the given levels into histogram, whose size is the
/// max level plus one.
PARQUET_EXPORT
void updateLevelHistogram(
    const int16_t* levels,
    int64_t numLevels,
    int64_t* histogram,
    int64_t histogramSize);

} // namespace facebook::velox::parquet::arrow
