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

#include "velox/dwio/parquet/writer/Writer.h"

#include <algorithm>
#include <cstring>
#include <exception>

#include <arrow/c/bridge.h>
#include <arrow/io/interfaces.h>
#include <arrow/table.h>
#include "velox/common/Casts.h"
#include "velox/common/base/Pointers.h"
#include "velox/common/config/Config.h"
#include "velox/common/testutil/TestValue.h"
#include "velox/dwio/parquet/common/ParquetConfig.h"
#include "velox/dwio/parquet/writer/arrow/ArrowSchema.h"
#include "velox/dwio/parquet/writer/arrow/Properties.h"
#include "velox/dwio/parquet/writer/arrow/Writer.h"
#include "velox/exec/MemoryReclaimer.h"

namespace facebook::velox::parquet {

using facebook::velox::parquet::arrow::ArrowWriterProperties;
using facebook::velox::parquet::arrow::Compression;
using facebook::velox::parquet::arrow::WriterProperties;
using facebook::velox::parquet::arrow::arrow::FileWriter;

// Utility for buffering Arrow output with a DataBuffer.
class ArrowDataBufferSink : public ::arrow::io::OutputStream {
 public:
  /// @param growRatio Growth factor used when invoking the reserve() method of
  /// DataSink, thereby helping to minimize frequent memcpy operations.
  ArrowDataBufferSink(
      std::unique_ptr<dwio::common::FileSink> sink,
      memory::MemoryPool& pool,
      double growRatio)
      : sink_(std::move(sink)), growRatio_(growRatio), buffer_(pool) {}

  ::arrow::Status Write(const std::shared_ptr<::arrow::Buffer>& data) override {
    auto requestCapacity = buffer_.size() + data->size();
    if (requestCapacity > buffer_.capacity()) {
      buffer_.reserve(growRatio_ * (requestCapacity));
    }
    buffer_.append(
        buffer_.size(),
        reinterpret_cast<const char*>(data->data()),
        data->size());
    return ::arrow::Status::OK();
  }

  ::arrow::Status Write(const void* data, int64_t nbytes) override {
    auto requestCapacity = buffer_.size() + nbytes;
    if (requestCapacity > buffer_.capacity()) {
      buffer_.reserve(growRatio_ * (requestCapacity));
    }
    buffer_.append(buffer_.size(), reinterpret_cast<const char*>(data), nbytes);
    return ::arrow::Status::OK();
  }

  ::arrow::Status Flush() override {
    bytesFlushed_ += buffer_.size();
    sink_->write(std::move(buffer_));
    return ::arrow::Status::OK();
  }

  ::arrow::Result<int64_t> Tell() const override {
    return bytesFlushed_ + buffer_.size();
  }

  ::arrow::Status Close() override {
    ARROW_RETURN_NOT_OK(Flush());
    sink_->close();
    return ::arrow::Status::OK();
  }

  bool closed() const override {
    return sink_->isClosed();
  }

  void abort() {
    sink_.reset();
    buffer_.clear();
  }

 private:
  std::unique_ptr<dwio::common::FileSink> sink_;
  const double growRatio_;
  dwio::common::DataBuffer<char> buffer_;
  int64_t bytesFlushed_ = 0;
};

struct ArrowContext {
  std::unique_ptr<FileWriter> writer;
  std::shared_ptr<::arrow::Schema> schema;
  std::shared_ptr<WriterProperties> properties;
  uint64_t stagingRows = 0;
  int64_t stagingBytes = 0;
  // columns, Arrays
  std::vector<std::vector<std::shared_ptr<::arrow::Array>>> stagingChunks;
};

Compression::type getArrowParquetCompression(
    common::CompressionKind compression) {
  if (compression == common::CompressionKind_SNAPPY) {
    return Compression::SNAPPY;
  } else if (compression == common::CompressionKind_GZIP) {
    return Compression::GZIP;
  } else if (compression == common::CompressionKind_ZSTD) {
    return Compression::ZSTD;
  } else if (compression == common::CompressionKind_NONE) {
    return Compression::UNCOMPRESSED;
  } else if (compression == common::CompressionKind_LZ4) {
    return Compression::LZ4_HADOOP;
  } else {
    VELOX_FAIL("Unsupported compression {}", compression);
  }
}

namespace {

std::optional<TimestampPrecision> toTimestampPrecision(
    std::optional<uint8_t> unit) {
  if (!unit) {
    return std::nullopt;
  }
  VELOX_CHECK(
      *unit == 3 /*milli*/ || *unit == 6 /*micro*/ || *unit == 9 /*nano*/,
      "Invalid timestamp unit: {}",
      *unit);
  return static_cast<TimestampPrecision>(*unit);
}

// Converts a string to TimestampPrecision. Accepts numeric values "3" (milli),
// "6" (micro), or "9" (nano).
TimestampPrecision stringToTimestampPrecision(const std::string& value) {
  return toTimestampPrecision(std::optional{folly::to<uint8_t>(value)}).value();
}

const ParquetWriterOptions& getFormatOptions(
    const dwio::common::WriterOptions& options) {
  static const ParquetWriterOptions kDefaultOptions;
  if (!options.formatSpecificOptions) {
    return kDefaultOptions;
  }
  return *checkedPointerCast<ParquetWriterOptions>(
      options.formatSpecificOptions);
}

std::shared_ptr<WriterProperties> getArrowParquetWriterOptions(
    const dwio::common::WriterOptions& options,
    const ParquetWriterOptions& parquetOptions,
    const std::unique_ptr<DefaultFlushPolicy>& flushPolicy) {
  auto builder = WriterProperties::Builder();
  WriterProperties::Builder* properties = &builder;
  if (parquetOptions.enableDictionary.value_or(
          facebook::velox::parquet::arrow::DEFAULT_IS_DICTIONARY_ENABLED)) {
    properties = properties->enableDictionary();
    properties = properties->dictionaryPagesizeLimit(
        parquetOptions.dictionaryPageSizeLimit.value_or(
            facebook::velox::parquet::arrow::
                DEFAULT_DICTIONARY_PAGE_SIZE_LIMIT));
  } else {
    properties = properties->disableDictionary();
  }
  properties = properties->compression(getArrowParquetCompression(
      options.compressionKind.value_or(common::CompressionKind_NONE)));
  for (const auto& columnCompressionValues :
       parquetOptions.columnCompressionsMap) {
    properties->compression(
        columnCompressionValues.first,
        getArrowParquetCompression(columnCompressionValues.second));
  }
  properties = properties->encoding(parquetOptions.encoding);
  properties = properties->dataPagesize(parquetOptions.dataPageSize.value_or(
      facebook::velox::parquet::arrow::kDefaultDataPageSize));
  properties = properties->writeBatchSize(parquetOptions.batchSize.value_or(
      facebook::velox::parquet::arrow::DEFAULT_WRITE_BATCH_SIZE));
  properties = properties->maxRowGroupLength(
      static_cast<int64_t>(flushPolicy->rowsInRowGroup()));
  properties = properties->codecOptions(parquetOptions.codecOptions);
  if (parquetOptions.enableStoreDecimalAsInteger.value_or(true)) {
    properties = properties->enableStoreDecimalAsInteger();
  } else {
    properties = properties->disableStoreDecimalAsInteger();
  }
  if (parquetOptions.useParquetDataPageV2.value_or(false)) {
    properties = properties->dataPageVersion(arrow::ParquetDataPageVersion::V2);
  } else {
    properties = properties->dataPageVersion(arrow::ParquetDataPageVersion::V1);
  }
  if (parquetOptions.createdBy.has_value()) {
    properties = properties->createdBy(parquetOptions.createdBy.value());
  }
  return properties->build();
}

void validateSchemaRecursive(
    const RowTypePtr& schema,
    const std::vector<ParquetFieldId>& parquetFieldIds) {
  // Check the schema's field names are not empty and unique.
  VELOX_USER_CHECK_NOT_NULL(schema, "Schema must not be empty.");
  const auto& fieldNames = schema->names();

  folly::F14FastSet<std::string> uniqueNames;
  for (const auto& name : fieldNames) {
    VELOX_USER_CHECK(!name.empty(), "Field name must not be empty.");
    auto result = uniqueNames.insert(name);
    VELOX_USER_CHECK(
        result.second,
        "File schema should not have duplicate columns: {}",
        name);
  }

  if (!parquetFieldIds.empty()) {
    VELOX_USER_CHECK_EQ(parquetFieldIds.size(), schema->size());
  }

  for (auto i = 0; i < schema->size(); ++i) {
    const auto& childType = schema->childAt(i);
    const auto& childFieldIds =
        parquetFieldIds.empty() ? parquetFieldIds : parquetFieldIds[i].children;

    if (childType->isRow()) {
      validateSchemaRecursive(
          std::dynamic_pointer_cast<const RowType>(childType), childFieldIds);
    } else if (childType->isArray()) {
      if (!parquetFieldIds.empty()) {
        VELOX_USER_CHECK_EQ(parquetFieldIds[i].children.size(), 1);
      }
      const auto& elementType = childType->asArray().elementType();
      if (elementType->isRow()) {
        validateSchemaRecursive(
            std::dynamic_pointer_cast<const RowType>(elementType),
            childFieldIds.empty() ? childFieldIds : childFieldIds[0].children);
      }
    } else if (childType->isMap()) {
      if (!parquetFieldIds.empty()) {
        VELOX_USER_CHECK_EQ(parquetFieldIds[i].children.size(), 2);
      }
      const auto& mapType = childType->asMap();
      if (mapType.keyType()->isRow()) {
        validateSchemaRecursive(
            std::dynamic_pointer_cast<const RowType>(mapType.keyType()),
            childFieldIds.empty() ? childFieldIds : childFieldIds[0].children);
      }
      if (mapType.valueType()->isRow()) {
        validateSchemaRecursive(
            std::dynamic_pointer_cast<const RowType>(mapType.valueType()),
            childFieldIds.empty() ? childFieldIds : childFieldIds[1].children);
      }
    }
  }
}

std::shared_ptr<::arrow::Field> updateFieldNameAndIdRecursive(
    const std::shared_ptr<::arrow::Field>& field,
    const Type& type,
    const ParquetFieldId* fieldId,
    const std::string& name = "") {
  auto newField = name.empty() ? field : field->WithName(name);

  if (fieldId) {
    newField =
        newField->WithMetadata(arrow::arrow::fieldIdMetadata(fieldId->fieldId));
  }

  if (type.isRow()) {
    auto& rowType = type.asRow();
    auto structType =
        std::dynamic_pointer_cast<::arrow::StructType>(newField->type());
    auto childrenSize = rowType.size();
    VELOX_CHECK(!fieldId || childrenSize <= fieldId->children.size());
    std::vector<std::shared_ptr<::arrow::Field>> newFields;
    newFields.reserve(childrenSize);
    for (auto i = 0; i < childrenSize; ++i) {
      const auto* childSetting = fieldId ? &fieldId->children.at(i) : nullptr;
      newFields.push_back(updateFieldNameAndIdRecursive(
          structType->fields()[i],
          *rowType.childAt(i),
          childSetting,
          rowType.nameOf(i)));
    }
    newField = newField->WithType(::arrow::struct_(newFields));
  } else if (type.isArray()) {
    auto listType =
        std::dynamic_pointer_cast<::arrow::BaseListType>(newField->type());
    auto elementType = type.asArray().elementType();
    auto elementField = listType->value_field();
    const auto* childSetting = fieldId ? &fieldId->children.at(0) : nullptr;
    auto updatedElementField =
        updateFieldNameAndIdRecursive(elementField, *elementType, childSetting);
    newField = newField->WithType(::arrow::list(updatedElementField));
  } else if (type.isMap()) {
    auto mapType = type.asMap();
    auto arrowMapType =
        std::dynamic_pointer_cast<::arrow::MapType>(newField->type());
    const auto* keySetting = fieldId ? &fieldId->children.at(0) : nullptr;
    const auto* valueSetting = fieldId ? &fieldId->children.at(1) : nullptr;
    auto newKeyField = updateFieldNameAndIdRecursive(
        arrowMapType->key_field(), *mapType.keyType(), keySetting);
    auto newValueField = updateFieldNameAndIdRecursive(
        arrowMapType->item_field(), *mapType.valueType(), valueSetting);
    newField = newField->WithType(
        std::make_shared<::arrow::MapType>(newKeyField, newValueField));
  }

  return newField;
}

std::optional<bool> isParquetV2(std::optional<std::string> version) {
  if (!version) {
    return std::nullopt;
  }
  if (version == "V1") {
    return false;
  }
  if (version == "V2") {
    return true;
  }
  VELOX_FAIL("Unsupported parquet datapage version {}", *version);
}

std::optional<int64_t> toParquetPageSize(std::optional<std::string> pageSize) {
  if (!pageSize) {
    return std::nullopt;
  }
  return config::toCapacity(*pageSize, config::CapacityUnit::BYTE);
}

std::optional<bool> toBoolConfigValue(
    std::optional<std::string> value,
    const char* optionName) {
  if (!value) {
    return std::nullopt;
  }
  try {
    return folly::to<bool>(*value);
  } catch (const std::exception& e) {
    VELOX_USER_FAIL(
        "Invalid parquet writer {} option: {}", optionName, e.what());
  }
}

std::optional<int64_t> toParquetBatchSize(
    std::optional<std::string> batchSize) {
  if (!batchSize) {
    return std::nullopt;
  }
  try {
    return folly::to<int64_t>(*batchSize);
  } catch (const std::exception& e) {
    VELOX_USER_FAIL("Invalid parquet writer batch size: {}", e.what());
  }
}

} // namespace

Writer::Writer(
    std::unique_ptr<dwio::common::FileSink> sink,
    const dwio::common::WriterOptions& options,
    std::shared_ptr<memory::MemoryPool> pool,
    RowTypePtr schema)
    : pool_(std::move(pool)),
      generalPool_{pool_->addLeafChild(".general")},
      stream_(
          std::make_shared<ArrowDataBufferSink>(
              std::move(sink),
              *generalPool_,
              getFormatOptions(options).bufferGrowRatio)),
      arrowContext_(std::make_shared<ArrowContext>()),
      schema_(std::move(schema)) {
  const auto& parquetWriterOptions = getFormatOptions(options);
  validateSchemaRecursive(schema_, parquetWriterOptions.parquetFieldIds);
  auto parquetWriteTimestampUnit =
      parquetWriterOptions.parquetWriteTimestampUnit;
  if (const auto serdeTimestampUnitIt = options.serdeParameters.find(
          std::string(ParquetConfig::kWriterSerdeTimestampUnit));
      serdeTimestampUnitIt != options.serdeParameters.end()) {
    parquetWriteTimestampUnit =
        stringToTimestampPrecision(serdeTimestampUnitIt->second);
  }
  auto parquetWriteTimestampTimeZone =
      parquetWriterOptions.parquetWriteTimestampTimeZone;
  if (const auto serdeTimestampTimezoneIt = options.serdeParameters.find(
          std::string(ParquetConfig::kWriterSerdeTimestampTimezone));
      serdeTimestampTimezoneIt != options.serdeParameters.end()) {
    parquetWriteTimestampTimeZone = serdeTimestampTimezoneIt->second.empty()
        ? std::optional<std::string>{std::nullopt}
        : std::optional<std::string>{serdeTimestampTimezoneIt->second};
  } else if (
      !parquetWriteTimestampTimeZone.has_value() &&
      !options.sessionTimezoneName.empty()) {
    parquetWriteTimestampTimeZone = options.sessionTimezoneName;
  }

  if (options.flushPolicyFactory) {
    castUniquePointer(options.flushPolicyFactory(), flushPolicy_);
  } else if (options.maxTargetFileSizeBytes > 0) {
    auto bytesInRowGroup = static_cast<int64_t>(std::min<uint64_t>(
        DefaultFlushPolicy::kDefaultBytesInRowGroup,
        options.maxTargetFileSizeBytes));
    flushPolicy_ = std::make_unique<DefaultFlushPolicy>(
        DefaultFlushPolicy::kDefaultRowsInGroup, bytesInRowGroup);
  } else {
    flushPolicy_ = std::make_unique<DefaultFlushPolicy>();
  }
  options_.timestampUnit = static_cast<TimestampUnit>(
      parquetWriteTimestampUnit.value_or(TimestampPrecision::kNanoseconds));
  options_.timestampTimeZone = parquetWriteTimestampTimeZone;
  common::testutil::TestValue::adjust(
      "facebook::velox::parquet::Writer::Writer", &options_);
  arrowContext_->properties =
      getArrowParquetWriterOptions(options, parquetWriterOptions, flushPolicy_);
  setMemoryReclaimers();
  writeInt96AsTimestamp_ = parquetWriterOptions.writeInt96AsTimestamp;
  arrowMemoryPool_ = parquetWriterOptions.arrowMemoryPool;
  parquetFieldIds_ = parquetWriterOptions.parquetFieldIds;
}

Writer::Writer(
    std::unique_ptr<dwio::common::FileSink> sink,
    const dwio::common::WriterOptions& options,
    RowTypePtr schema)
    : Writer{
          std::move(sink),
          options,
          options.memoryPool->addAggregateChild(
              fmt::format(
                  "writer_node_{}",
                  folly::to<std::string>(folly::Random::rand64()))),
          std::move(schema)} {}

void Writer::flush() {
  if (arrowContext_->stagingRows > 0) {
    if (!arrowContext_->writer) {
      ArrowWriterProperties::Builder builder;
      if (writeInt96AsTimestamp_) {
        builder.enableDeprecatedInt96Timestamps();
      }
      auto arrowProperties = builder.build();
      PARQUET_ASSIGN_OR_THROW(
          arrowContext_->writer,
          FileWriter::open(
              *arrowContext_->schema.get(),
              arrowMemoryPool_.get(),
              stream_,
              arrowContext_->properties,
              arrowProperties));
    }

    auto fields = arrowContext_->schema->fields();
    std::vector<std::shared_ptr<::arrow::ChunkedArray>> chunks;
    for (int colIdx = 0; colIdx < fields.size(); colIdx++) {
      auto dataType = fields.at(colIdx)->type();
      auto chunk =
          ::arrow::ChunkedArray::Make(
              std::move(arrowContext_->stagingChunks.at(colIdx)), dataType)
              .ValueOrDie();
      chunks.push_back(chunk);
    }
    auto table = ::arrow::Table::Make(
        arrowContext_->schema,
        std::move(chunks),
        static_cast<int64_t>(arrowContext_->stagingRows));
    PARQUET_THROW_NOT_OK(arrowContext_->writer->writeTable(
        *table, static_cast<int64_t>(flushPolicy_->rowsInRowGroup())));
    PARQUET_THROW_NOT_OK(stream_->Flush());
    for (auto& chunk : arrowContext_->stagingChunks) {
      chunk.clear();
    }
    arrowContext_->stagingRows = 0;
    arrowContext_->stagingBytes = 0;
  }
}

dwio::common::StripeProgress getStripeProgress(
    uint64_t stagingRows,
    int64_t stagingBytes) {
  return dwio::common::StripeProgress{
      .stripeRowCount = stagingRows, .stripeSizeEstimate = stagingBytes};
}

/**
 * This method would cache input `ColumnarBatch` to make the size of row group
 * big. It would flush when:
 * - the cached numRows bigger than `rowsInRowGroup_`
 * - the cached bytes bigger than `bytesInRowGroup_`
 *
 * This method assumes each input `ColumnarBatch` have same schema.
 */
void Writer::write(const VectorPtr& data) {
  VELOX_USER_CHECK(
      data->type()->equivalent(*schema_),
      "The file schema type should be equal with the input rowvector type.");

  VectorPtr exportData = flattenIfNeeded(data);

  ArrowArray array;
  ArrowSchema schema;
  exportToArrow(exportData, array, generalPool_.get(), options_);
  exportToArrow(exportData, schema, options_);

  // Convert the arrow schema to Schema and then update the column names based
  // on schema_.
  auto arrowSchema = ::arrow::ImportSchema(&schema).ValueOrDie();
  common::testutil::TestValue::adjust(
      "facebook::velox::parquet::Writer::write", arrowSchema.get());
  std::vector<std::shared_ptr<::arrow::Field>> newFields;
  auto childSize = schema_->size();
  if (!parquetFieldIds_.empty()) {
    VELOX_CHECK(childSize == parquetFieldIds_.size());
  }
  for (auto i = 0; i < childSize; i++) {
    newFields.push_back(updateFieldNameAndIdRecursive(
        arrowSchema->fields()[i],
        *schema_->childAt(i),
        !parquetFieldIds_.empty() ? &parquetFieldIds_.at(i) : nullptr,
        schema_->nameOf(i)));
  }

  PARQUET_ASSIGN_OR_THROW(
      auto recordBatch,
      ::arrow::ImportRecordBatch(&array, ::arrow::schema(newFields)));
  if (!arrowContext_->schema) {
    arrowContext_->schema = recordBatch->schema();
    for (int colIdx = 0; colIdx < arrowContext_->schema->num_fields();
         colIdx++) {
      arrowContext_->stagingChunks.push_back(
          std::vector<std::shared_ptr<::arrow::Array>>());
    }
  }

  // Use retainedSize() rather than estimateFlatSize() for the flush policy
  // size estimate. estimateFlatSize() computes the fully materialized size of
  // dictionary columns (rows * avgValueSize), which greatly overestimates the
  // actual data written when dictionaries pass through without flattening.
  // retainedSize() reports the actual memory footprint (dictionary values +
  // index buffer), matching what is encoded in the Parquet file.
  auto bytes = data->retainedSize();
  auto numRows = data->size();
  if (flushPolicy_->shouldFlush(getStripeProgress(
          arrowContext_->stagingRows, arrowContext_->stagingBytes))) {
    flush();
  }

  for (int colIdx = 0; colIdx < recordBatch->num_columns(); colIdx++) {
    arrowContext_->stagingChunks.at(colIdx).push_back(
        recordBatch->column(colIdx));
  }
  arrowContext_->stagingRows += numRows;
  arrowContext_->stagingBytes += bytes;
}

bool Writer::isCodecAvailable(common::CompressionKind compression) {
  return arrow::util::Codec::isAvailable(
      getArrowParquetCompression(compression));
}

void Writer::newRowGroup(int32_t numRows) {
  PARQUET_THROW_NOT_OK(arrowContext_->writer->newRowGroup(numRows));
}

std::unique_ptr<dwio::common::FileMetadata> Writer::close() {
  flush();

  std::unique_ptr<ParquetFileMetadata> parquetFileMetadata;
  if (arrowContext_->writer) {
    PARQUET_THROW_NOT_OK(arrowContext_->writer->close());
    parquetFileMetadata = std::make_unique<ParquetFileMetadata>(
        arrowContext_->writer->metadata());
    arrowContext_->writer.reset();
  }

  PARQUET_THROW_NOT_OK(stream_->Close());

  arrowContext_->stagingChunks.clear();

  return parquetFileMetadata;
}

void Writer::abort() {
  stream_->abort();
  arrowContext_.reset();
}

void Writer::setMemoryReclaimers() {
  VELOX_CHECK(
      !pool_->isLeaf(),
      "The root memory pool for parquet writer can't be leaf: {}",
      pool_->name());
  VELOX_CHECK_NULL(pool_->reclaimer());

  if ((pool_->parent() == nullptr) ||
      (pool_->parent()->reclaimer() == nullptr)) {
    return;
  }

  // TODO https://github.com/facebookincubator/velox/issues/8190
  pool_->setReclaimer(exec::MemoryReclaimer::create());
  generalPool_->setReclaimer(exec::MemoryReclaimer::create());
}

namespace {

/// Returns true if a single column requires flattening before Arrow export.
bool childNeedsFlatten(const VectorPtr& child) {
  auto encoding = child->encoding();
  if (encoding == VectorEncoding::Simple::DICTIONARY) {
    auto* innerVector = child->valueVector().get();
    if (innerVector != nullptr) {
      // Flatten dictionary wrapping a complex (non-primitive) type. The Arrow
      // Parquet writer only supports dictionary passthrough for binary-like
      // scalar types, and updateFieldNameAndIdRecursive cannot traverse
      // DictionaryType wrapping complex Arrow types.
      if (!innerVector->type()->isPrimitiveType()) {
        return true;
      }
      // Flatten nested wrapping (e.g., dictionary-of-dictionary,
      // dictionary-of-constant). Only a dictionary directly wrapping a flat
      // vector can be exported as an Arrow DictionaryArray.
      if (!innerVector->isFlatEncoding()) {
        return true;
      }
      // Flatten if the dictionary values contain nulls. Arrow's Parquet
      // writer does not support DictionaryArray with nulls encoded in the
      // dictionary values — nulls must be represented in the indices layer.
      if (innerVector->mayHaveNulls()) {
        return true;
      }
      // Flatten "selection dictionaries" where the dictionary is larger than
      // the vector (e.g., exec::wrap selecting a partition's rows from a full
      // batch). Passing these through to Arrow is counterproductive: Arrow's
      // putDictionary hashes all dictionary entries, detects duplicates, and
      // falls back to plain encoding — wasting work. Flattening up front is
      // cheaper because it only materializes the selected rows.
      //
      // Also flatten when the dictionary exceeds kMaxPassthroughDictSize.
      // Arrow's putDictionary inserts each entry individually into its memo
      // table, while its flat-data batch path uses more efficient vectorized
      // encoding. Benchmarks show the crossover at ~1000-10000 entries;
      // 4096 sits in the neutral zone. Note that Parquet/Spark/Arrow all use
      // a 1MB *byte-size* limit for their own dictionary encoding decisions,
      // but that governs Parquet-level encoding, not the Velox-to-Arrow
      // handoff efficiency that this threshold addresses.
      constexpr vector_size_t kMaxPassthroughDictSize = 4'096;
      if (innerVector->size() > child->size() ||
          innerVector->size() > kMaxPassthroughDictSize) {
        return true;
      }
    }
  } else if (encoding == VectorEncoding::Simple::CONSTANT) {
    // Flatten constant wrapping a non-flat inner vector
    // (e.g., constant-of-dictionary).
    if (child->valueVector() && !child->wrappedVector()->isFlatEncoding()) {
      return true;
    }
  }
  return false;
}

/// Returns true if a dictionary child should have its indices detached (copied)
/// before Arrow export. This is needed because the Arrow bridge stores a
/// shared reference to the indices buffer; if an external owner (e.g.,
/// FileDataSink's partitionRows_) later overwrites that buffer for a
/// subsequent batch, the staged Arrow array would be silently corrupted.
/// Copying just the indices (N * sizeof(int32_t)) is cheap compared to a
/// full flatten that materializes the dictionary values.
bool childNeedsDetach(const VectorPtr& child) {
  return child->encoding() == VectorEncoding::Simple::DICTIONARY &&
      !childNeedsFlatten(child);
}

} // namespace

VectorPtr Writer::flattenIfNeeded(const VectorPtr& data) const {
  auto rowVector = std::dynamic_pointer_cast<RowVector>(data);
  VELOX_CHECK_NOT_NULL(
      rowVector, "Arrow export expects a RowVector as input data.");

  const auto& children = rowVector->children();
  bool anyNeedsWork = false;
  for (const auto& child : children) {
    if (childNeedsFlatten(child) || childNeedsDetach(child)) {
      anyNeedsWork = true;
      break;
    }
  }

  if (!anyNeedsWork) {
    return data;
  }

  // Selectively flatten or detach only the columns that need it.
  std::vector<VectorPtr> newChildren(children.size());
  for (size_t i = 0; i < children.size(); ++i) {
    if (childNeedsFlatten(children[i])) {
      newChildren[i] = children[i];
      BaseVector::flattenVector(newChildren[i]);
    } else if (childNeedsDetach(children[i])) {
      // Copy only the indices buffer to prevent external mutation from
      // corrupting staged Arrow data. The dictionary values are shared
      // (zero-copy) since they are immutable string data.
      const auto& dict = children[i];
      auto srcIndices = dict->wrapInfo();
      const auto numBytes = dict->size() * sizeof(vector_size_t);
      auto newIndices =
          AlignedBuffer::allocate<vector_size_t>(dict->size(), generalPool_.get());
      std::memcpy(
          newIndices->asMutable<char>(), srcIndices->as<char>(), numBytes);
      newChildren[i] = BaseVector::wrapInDictionary(
          dict->nulls(), std::move(newIndices), dict->size(),
          dict->valueVector());
    } else {
      newChildren[i] = children[i];
    }
  }

  return std::make_shared<RowVector>(
      rowVector->pool(),
      rowVector->type(),
      rowVector->nulls(),
      rowVector->size(),
      std::move(newChildren));
}

std::unique_ptr<dwio::common::Writer> ParquetWriterFactory::createWriter(
    std::unique_ptr<dwio::common::FileSink> sink,
    const std::shared_ptr<dwio::common::WriterOptions>& options) {
  return std::make_unique<Writer>(
      std::move(sink), *options, asRowType(options->schema));
}

std::unique_ptr<dwio::common::WriterOptions>
ParquetWriterFactory::createWriterOptions() {
  return std::make_unique<dwio::common::WriterOptions>();
}

std::shared_ptr<dwio::common::FormatSpecificOptions>
ParquetWriterFactory::createFormatOptions(
    const config::ConfigBase& connectorConfig,
    const config::ConfigBase& session) const {
  auto parquetOptions = std::make_shared<ParquetWriterOptions>();
  parquetOptions->parquetWriteTimestampUnit = toTimestampPrecision(
      ParquetConfig::writerTimestampUnit(connectorConfig, session));
  parquetOptions->enableDictionary = toBoolConfigValue(
      ParquetConfig::writerEnableDictionary(connectorConfig, session),
      "enable dictionary");
  parquetOptions->enableStoreDecimalAsInteger = toBoolConfigValue(
      ParquetConfig::writerEnableStoreDecimalAsInteger(
          connectorConfig, session),
      "enable store decimal as integer");
  parquetOptions->dictionaryPageSizeLimit = toParquetPageSize(
      ParquetConfig::writerDictionaryPageSizeLimit(connectorConfig, session));
  parquetOptions->useParquetDataPageV2 = isParquetV2(
      ParquetConfig::writerDataPageVersion(connectorConfig, session));
  parquetOptions->dataPageSize = toParquetPageSize(
      ParquetConfig::writerPageSize(connectorConfig, session));
  parquetOptions->batchSize = toParquetBatchSize(
      ParquetConfig::writerBatchSize(connectorConfig, session));
  parquetOptions->createdBy = ParquetConfig::writerCreatedBy(connectorConfig);
  return parquetOptions;
}

} // namespace facebook::velox::parquet
