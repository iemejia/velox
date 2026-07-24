# Copyright (c) Facebook, Inc. and its affiliates.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
include_guard(GLOBAL)

# Header-only. Pinned to the same pre-release commit Apache Arrow 23 uses (see
# arrow cpp/thirdparty/versions.txt): required to build with modern GCC without
# the issues fixed by https://github.com/Tencent/rapidjson/pull/1323. Consumed by
# the vendored Parquet writer's geospatial statistics (GeospatialUtilJsonInternal).
set(VELOX_RAPIDJSON_VERSION 232389d4f1012dddec4ef84861face2d2ba85709)
set(
  VELOX_RAPIDJSON_BUILD_SHA256_CHECKSUM
  b9290a9a6d444c8e049bd589ab804e0ccf2b05dc5984a19ed5ae75d090064806
)
set(
  VELOX_RAPIDJSON_SOURCE_URL
  "https://github.com/miloyip/rapidjson/archive/${VELOX_RAPIDJSON_VERSION}.tar.gz"
)

velox_resolve_dependency_url(RAPIDJSON)

message(STATUS "Building RapidJSON from source")
FetchContent_Declare(
  rapidjson
  URL ${VELOX_RAPIDJSON_SOURCE_URL}
  URL_HASH ${VELOX_RAPIDJSON_BUILD_SHA256_CHECKSUM}
)
# Header-only: do not configure/build RapidJSON's own tests/examples/docs.
set(RAPIDJSON_BUILD_TESTS OFF)
set(RAPIDJSON_BUILD_EXAMPLES OFF)
set(RAPIDJSON_BUILD_DOC OFF)
FetchContent_Populate(rapidjson)
FetchContent_GetProperties(rapidjson SOURCE_DIR RAPIDJSON_SOURCE_DIR)
find_path(
  RAPIDJSON_INCLUDE_DIR
  NAMES rapidjson/document.h
  PATHS ${RAPIDJSON_SOURCE_DIR}
  PATH_SUFFIXES include
)
