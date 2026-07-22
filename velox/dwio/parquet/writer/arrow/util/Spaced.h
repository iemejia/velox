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

#include <cassert>
#include <cstdint>
#include <cstring>

#include "arrow/util/bit_run_reader.h"

// Replaces arrow::util::internal::SpacedCompress/SpacedExpand, which Apache
// Arrow moved into the non-installed internal header
// arrow/util/spaced_internal.h after the version this writer was adapted from.
// The compaction/expansion is re-expressed on top of Arrow's still-public
// SetBitRunReader primitives.
namespace facebook::velox::parquet::arrow::util::internal {

/// Compresses the buffer to spaced, excluding the null entries. Copies only the
/// valid slots (as indicated by validBits) contiguously into output and returns
/// the number of valid values written.
template <typename T>
inline int spacedCompress(
    const T* src,
    int numValues,
    const uint8_t* validBits,
    int64_t validBitsOffset,
    T* output) {
  int numValidValues = 0;

  ::arrow::internal::SetBitRunReader reader(
      validBits, validBitsOffset, numValues);
  while (true) {
    const auto run = reader.NextRun();
    if (run.length == 0) {
      break;
    }
    std::memcpy(
        output + numValidValues, src + run.position, run.length * sizeof(T));
    numValidValues += static_cast<int32_t>(run.length);
  }

  return numValidValues;
}

/// Relocates values in buffer into the positions of non-null values as
/// indicated by a validity bitmap. Returns the number of values expanded,
/// including nulls.
template <typename T>
inline int spacedExpand(
    T* buffer,
    int numValues,
    int nullCount,
    const uint8_t* validBits,
    int64_t validBitsOffset) {
  // Point to end as we add the spacing from the back.
  int idxDecode = numValues - nullCount;

  // Depending on the number of nulls, some of the value slots in buffer may be
  // uninitialized, and this would cause valgrind warnings / potentially UB.
  std::memset(static_cast<void*>(buffer + idxDecode), 0, nullCount * sizeof(T));
  if (idxDecode == 0) {
    // All nulls, nothing more to do.
    return numValues;
  }

  ::arrow::internal::ReverseSetBitRunReader reader(
      validBits, validBitsOffset, numValues);
  while (true) {
    const auto run = reader.NextRun();
    if (run.length == 0) {
      break;
    }
    idxDecode -= static_cast<int32_t>(run.length);
    assert(idxDecode >= 0);
    std::memmove(
        buffer + run.position, buffer + idxDecode, run.length * sizeof(T));
  }

  // Otherwise caller gave an incorrect nullCount.
  assert(idxDecode == 0);
  return numValues;
}

} // namespace facebook::velox::parquet::arrow::util::internal
