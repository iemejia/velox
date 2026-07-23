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

#include "velox/dwio/parquet/writer/arrow/SizeStatistics.h"

#include <algorithm>
#include <functional>
#include <sstream>

#include "velox/dwio/parquet/writer/arrow/Exception.h"
#include "velox/dwio/parquet/writer/arrow/Schema.h"
#include "velox/dwio/parquet/writer/arrow/Types.h"

namespace facebook::velox::parquet::arrow {

namespace {

void mergeLevelHistogram(
    std::vector<int64_t>& histogram,
    const std::vector<int64_t>& other) {
  VELOX_DCHECK_EQ(histogram.size(), other.size());
  std::transform(
      histogram.begin(),
      histogram.end(),
      other.begin(),
      histogram.begin(),
      std::plus<>());
}

} // namespace

void SizeStatistics::merge(const SizeStatistics& other) {
  if (repetitionLevelHistogram.size() !=
      other.repetitionLevelHistogram.size()) {
    throw ParquetException("Repetition level histogram size mismatch");
  }
  if (definitionLevelHistogram.size() !=
      other.definitionLevelHistogram.size()) {
    throw ParquetException("Definition level histogram size mismatch");
  }
  if (unencodedByteArrayDataBytes.has_value() !=
      other.unencodedByteArrayDataBytes.has_value()) {
    throw ParquetException(
        "Unencoded byte array data bytes are not consistent");
  }
  mergeLevelHistogram(repetitionLevelHistogram, other.repetitionLevelHistogram);
  mergeLevelHistogram(definitionLevelHistogram, other.definitionLevelHistogram);
  if (unencodedByteArrayDataBytes.has_value()) {
    unencodedByteArrayDataBytes = unencodedByteArrayDataBytes.value() +
        other.unencodedByteArrayDataBytes.value();
  }
}

void SizeStatistics::incrementUnencodedByteArrayDataBytes(int64_t value) {
  VELOX_CHECK(unencodedByteArrayDataBytes.has_value());
  unencodedByteArrayDataBytes = unencodedByteArrayDataBytes.value() + value;
}

void SizeStatistics::validate(const ColumnDescriptor* descr) const {
  auto validateHistogram = [](const std::vector<int64_t>& histogram,
                              int16_t maxLevel,
                              const std::string& name) {
    if (histogram.empty()) {
      // A level histogram is always allowed to be missing.
      return;
    }
    if (histogram.size() != static_cast<size_t>(maxLevel + 1)) {
      std::stringstream ss;
      ss << name << " level histogram size mismatch, size: " << histogram.size()
         << ", expected: " << (maxLevel + 1);
      throw ParquetException(ss.str());
    }
  };
  validateHistogram(
      repetitionLevelHistogram, descr->maxRepetitionLevel(), "Repetition");
  validateHistogram(
      definitionLevelHistogram, descr->maxDefinitionLevel(), "Definition");
  if (unencodedByteArrayDataBytes.has_value() &&
      descr->physicalType() != Type::kByteArray) {
    throw ParquetException(
        "Unencoded byte array data bytes is only supported for BYTE_ARRAY "
        "columns");
  }
}

void SizeStatistics::reset() {
  repetitionLevelHistogram.assign(repetitionLevelHistogram.size(), 0);
  definitionLevelHistogram.assign(definitionLevelHistogram.size(), 0);
  if (unencodedByteArrayDataBytes.has_value()) {
    unencodedByteArrayDataBytes = 0;
  }
}

std::unique_ptr<SizeStatistics> SizeStatistics::make(
    const ColumnDescriptor* descr) {
  auto sizeStats = std::make_unique<SizeStatistics>();
  // If the max level is 0, the level histogram can be omitted because it
  // contains only a single level whose count equals the number of values.
  if (descr->maxRepetitionLevel() != 0) {
    sizeStats->repetitionLevelHistogram.resize(
        descr->maxRepetitionLevel() + 1, 0);
  }
  if (descr->maxDefinitionLevel() != 0) {
    sizeStats->definitionLevelHistogram.resize(
        descr->maxDefinitionLevel() + 1, 0);
  }
  if (descr->physicalType() == Type::kByteArray) {
    sizeStats->unencodedByteArrayDataBytes = 0;
  }
  return sizeStats;
}

void updateLevelHistogram(
    const int16_t* levels,
    int64_t numLevels,
    int64_t* histogram,
    int64_t histogramSize) {
  VELOX_DCHECK_GE(histogramSize, 1);
  const int16_t maxLevel = static_cast<int16_t>(histogramSize - 1);
  if (maxLevel == 0) {
    histogram[0] += numLevels;
    return;
  }
  for (int64_t i = 0; i < numLevels; ++i) {
    const int16_t level = levels[i];
    VELOX_DCHECK_LE(level, maxLevel);
    ++histogram[level];
  }
}

} // namespace facebook::velox::parquet::arrow
