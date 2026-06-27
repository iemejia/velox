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

#include "velox/common/base/Nulls.h"

namespace facebook::velox::parquet {

/// Decode BYTE_STREAM_SPLIT encoded columns from Parquet.
///
/// This encoding splits each value into its constituent bytes and stores
/// all byte-0s contiguously, then all byte-1s, etc. For N values of a
/// K-byte type, the encoded layout is:
///
///   [byte 0 of val 0] [byte 0 of val 1] ... [byte 0 of val N-1]
///   [byte 1 of val 0] [byte 1 of val 1] ... [byte 1 of val N-1]
///   ...
///   [byte K-1 of val 0] ... [byte K-1 of val N-1]
///
/// Supported for FLOAT (4 bytes) and DOUBLE (8 bytes).
template <typename T>
class ByteStreamSplitDecoder {
 public:
  static_assert(
      sizeof(T) == 4 || sizeof(T) == 8,
      "BYTE_STREAM_SPLIT only supports 4-byte and 8-byte types");

  ByteStreamSplitDecoder(const char* data, uint32_t size)
      : data_(reinterpret_cast<const uint8_t*>(data)),
        numValues_(size / sizeof(T)),
        position_(0) {}

  /// Reset to decode from a new page buffer without reallocating.
  void reset(const char* data, uint32_t size) {
    data_ = reinterpret_cast<const uint8_t*>(data);
    numValues_ = size / sizeof(T);
    position_ = 0;
  }

  void skip(uint64_t numValues) {
    position_ += numValues;
  }

  template <bool hasNulls>
  inline void skip(int32_t numValues, int32_t current, const uint64_t* nulls) {
    if (hasNulls) {
      numValues = bits::countNonNulls(nulls, current, current + numValues);
    }
    position_ += numValues;
  }

  template <bool hasNulls, typename Visitor>
  void readWithVisitor(const uint64_t* nulls, Visitor visitor) {
    int32_t current = visitor.start();
    skip<hasNulls>(current, 0, nulls);
    int32_t toSkip;
    bool atEnd = false;
    const bool allowNulls = hasNulls && visitor.allowNulls();
    for (;;) {
      if (hasNulls && allowNulls && bits::isBitNull(nulls, current)) {
        toSkip = visitor.processNull(atEnd);
      } else {
        if (hasNulls && !allowNulls) {
          toSkip = visitor.checkAndSkipNulls(nulls, current, atEnd);
          if (!Visitor::dense) {
            skip<false>(toSkip, current, nullptr);
          }
          if (atEnd) {
            return;
          }
        }

        // Read one value by gathering bytes from K streams.
        toSkip = visitor.process(readValue(), atEnd);
      }
      ++current;
      if (toSkip) {
        skip<hasNulls>(toSkip, current, nulls);
        current += toSkip;
      }
      if (atEnd) {
        return;
      }
    }
  }

 private:
  /// Gather the K bytes of one value from the split streams.
  T readValue() {
    T value;
    auto* bytes = reinterpret_cast<uint8_t*>(&value);
    for (size_t k = 0; k < sizeof(T); ++k) {
      bytes[k] = data_[position_ + k * numValues_];
    }
    ++position_;
    return value;
  }

  const uint8_t* data_;
  uint32_t numValues_;
  uint32_t position_;
};

} // namespace facebook::velox::parquet
