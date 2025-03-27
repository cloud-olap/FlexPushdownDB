//
// Created by Yifei Yang on 1/18/22.
//

#include <fpdb/tuple/util/Util.h>
#include <fpdb/tuple/ArrayAppenderWrapper.h>
#include <fmt/format.h>

namespace fpdb::tuple::util {

tl::expected<std::shared_ptr<arrow::Array>, std::string>
Util::makeEmptyArray(const std::shared_ptr<arrow::DataType> &type) {
  auto expAppender = ArrayAppenderBuilder::make(type);
  if (!expAppender.has_value()) {
    return tl::make_unexpected(expAppender.error());
  }
  const auto &appender = *expAppender;

  auto expArray = appender->finalize();
  if (!expArray.has_value()) {
    return tl::make_unexpected(expArray.error());
  }
  return *expArray;
}

tl::expected<std::shared_ptr<arrow::RecordBatch>, std::string>
Util::makeEmptyRecordBatch(const std::shared_ptr<arrow::Schema> &schema) {
  arrow::ArrayVector arrayVec;
  for (const auto &field: schema->fields()) {
    auto expArray = makeEmptyArray(field->type());
    if (!expArray.has_value()) {
      return tl::make_unexpected(expArray.error());
    }
    arrayVec.emplace_back(*expArray);
  }
  return arrow::RecordBatch::Make(schema, 0, arrayVec);
}

tl::expected<::arrow::RecordBatchVector, std::string>
Util::table_to_record_batches(const std::shared_ptr<arrow::Table> &table) {
  if (table == nullptr) {
    return tl::make_unexpected("Cannot make record batches from null table");
  }

  if (table->num_rows() > 0) {
    std::shared_ptr<arrow::RecordBatch> batch;
    ::arrow::TableBatchReader tbl_reader(*table);
    tbl_reader.set_chunksize(fpdb::tuple::DefaultChunkSize);
    auto expBatches = tbl_reader.ToRecordBatches();
    if (!expBatches.ok()) {
      return tl::make_unexpected(expBatches.status().message());
    }
    return *expBatches;
  } else {
    auto expRecordBatch = makeEmptyRecordBatch(table->schema());
    if (!expRecordBatch.has_value()) {
      return tl::make_unexpected(expRecordBatch.error());
    }
    return ::arrow::RecordBatchVector{*expRecordBatch};
  }
}

int64_t Util::getSize(const std::shared_ptr<arrow::RecordBatch> &recordBatch) {
  int64_t size = 0;
  for (const auto &array: recordBatch->columns()) {
    for (const auto &buffer: array->data()->buffers) {
      if (buffer) {
        size += buffer->size();
      }
    }
  }
  return size;
}

tl::expected<std::shared_ptr<arrow::Table>, std::string>
Util::calibrateSchema(const std::shared_ptr<arrow::Table> &table, const std::shared_ptr<arrow::Schema> &targetSchema) {
  // schemas are identical, just return
  if (table->schema()->Equals(targetSchema)) {
    return table;
  }

  // different number of fields
  if (table->schema()->num_fields() != targetSchema->num_fields()) {
    return tl::make_unexpected(fmt::format("Cannot calibrate schema, num fields are different, "
                                           "got '{}' but expected '{}'",
                                           table->schema()->num_fields(), targetSchema->num_fields()));
  }

  // schemas are not identical, try if we can rearrange columns to make them same
  std::unordered_map<std::string, std::shared_ptr<arrow::Field>> targetFieldMap;
  std::unordered_map<std::string, int> targetFieldIdMap;
  for (int i = 0; i < targetSchema->num_fields(); ++i) {
    const auto &field = targetSchema->field(i);
    targetFieldMap.emplace(field->name(), field);
    targetFieldIdMap.emplace(field->name(), i);
  }
  arrow::FieldVector rearrangedFields{(size_t) table->num_columns()};
  arrow::ChunkedArrayVector rearrangedColumns{(size_t) table->num_columns()};
  for (int i = 0; i < table->num_columns(); ++i) {
    const auto &field = table->schema()->field(i);
    const auto &column = table->column(i);
    // try to get field id
    auto targetFieldIt = targetFieldMap.find(field->name());
    if (targetFieldIt == targetFieldMap.end()) {
      return tl::make_unexpected(
              fmt::format("Cannot calibrate schema, field '{}' not exist in target schema", field->name()));
    }
    const auto &targetField = targetFieldIt->second;
    if (field->type()->id() != targetField->type()->id()) {
      return tl::make_unexpected(fmt::format("Cannot calibrate schema, field '{}' has different type in target schema, "
                                             "got '{}' but expected '{}'",
                                             field->name(), field->type()->name(), targetField->type()->name()));
    }
    int fieldId = targetFieldIdMap[field->name()];
    rearrangedFields[fieldId] = field;
    rearrangedColumns[fieldId] = column;
  }

  // rearrange is successful
  return arrow::Table::Make(arrow::schema(rearrangedFields), rearrangedColumns);
}

tl::expected<std::shared_ptr<arrow::Array>, std::string>
Util::copyArrayWhole(const std::shared_ptr<arrow::Array> &array, int numCopies) {
  // special case
  if (numCopies == 1) {
    return array;
  }
  // null values are currently not supported
  if (array->null_count() > 0) {
    return tl::make_unexpected("Copy whole array which has null values is unsupported");
  }
  switch (array->type_id()) {
    case arrow::Type::INT32:
      return copyArrayWholeNumeric<arrow::Int32Type>(array, numCopies);
    case arrow::Type::INT64:
      return copyArrayWholeNumeric<arrow::Int64Type>(array, numCopies);
    case arrow::Type::DOUBLE:
      return copyArrayWholeNumeric<arrow::DoubleType>(array, numCopies);
    case arrow::Type::DATE32:
      return copyArrayWholeNumeric<arrow::Date32Type>(array, numCopies);
    case arrow::Type::DATE64:
      return copyArrayWholeNumeric<arrow::Date64Type>(array, numCopies);
    case arrow::Type::STRING:
      return copyArrayWholeString(std::static_pointer_cast<arrow::StringArray>(array), numCopies);
    default: return tl::make_unexpected(
              fmt::format("Unsupported type to copy whole array, '{}'", array->type()->ToString()));
  }
}

tl::expected<std::shared_ptr<arrow::StringArray>, std::string>
Util::copyArrayWholeString(const std::shared_ptr<arrow::StringArray> &array, int numCopies) {
  using offsetType = arrow::StringType::offset_type;

  // info of original array
  int64_t length = array->data()->length;   // num elements
  int64_t offset = array->data()->offset;   // offset in data, in num elements
  offsetType* valueOffsets = (offsetType*) array->raw_value_offsets();    // offsets of each individual value
  const uint8_t* data = array->raw_data();

  // size in bytes
  int64_t bytesValueOffsets = (length + 1) * sizeof(offsetType);    // 1 extra offset at the tail
  offsetType bytesData = valueOffsets[offset + length] - valueOffsets[offset];

  // allocate new buffers
  int64_t newBytesValueOffsets = (length * numCopies + 1) * sizeof(offsetType);
  auto expNewValueOffsetsBuffer = arrow::AllocateBuffer(newBytesValueOffsets, arrow::default_memory_pool());
  if (!expNewValueOffsetsBuffer.ok()) {
    return tl::make_unexpected(expNewValueOffsetsBuffer.status().message());
  }
  std::shared_ptr<arrow::Buffer> newValueOffsetsBuffer = std::move(*expNewValueOffsetsBuffer);
  auto expNewDataBuffer = arrow::AllocateBuffer(bytesData * numCopies, arrow::default_memory_pool());
  if (!expNewDataBuffer.ok()) {
    return tl::make_unexpected(expNewDataBuffer.status().message());
  }
  std::shared_ptr<arrow::Buffer> newDataBuffer = std::move(*expNewDataBuffer);

  // assign offsets to new offsets buffer, handle the first copy specially
  memcpy(newValueOffsetsBuffer->mutable_data(), (uint8_t*) (valueOffsets + offset), bytesValueOffsets);
  offsetType* currWritePos = (offsetType*) (newValueOffsetsBuffer->mutable_data() + bytesValueOffsets);
  for (int i = 1; i < numCopies; ++i) {
    offsetType baseLength = bytesData * i;
    for (int j = 0; j < length; ++j) {
      currWritePos[j] = valueOffsets[offset + j + 1] + baseLength;
    }
    currWritePos += length;
  }

  // memcpy data to new data buffer
  int64_t bytesCopied = 0;
  for (int i = 0; i < numCopies; ++i) {
    memcpy(newDataBuffer->mutable_data() + bytesCopied, data + valueOffsets[offset], bytesData);
    bytesCopied += bytesData;
  }

  // make out array
  return std::make_shared<arrow::StringArray>(length * numCopies, newValueOffsetsBuffer, newDataBuffer);
}

tl::expected<std::shared_ptr<arrow::Array>, std::string>
Util::copyArrayRow(const std::shared_ptr<arrow::Array> &array, int numCopies) {
  // special case
  if (numCopies == 1) {
    return array;
  }
  // null values are currently not supported
  if (array->null_count() > 0) {
    return tl::make_unexpected("Copy array rows which has null values is unsupported");
  }
  switch (array->type_id()) {
    case arrow::Type::INT32:
      return copyArrayRowNumeric<arrow::Int32Type>(array, numCopies);
    case arrow::Type::INT64:
      return copyArrayRowNumeric<arrow::Int64Type>(array, numCopies);
    case arrow::Type::DOUBLE:
      return copyArrayRowNumeric<arrow::DoubleType>(array, numCopies);
    case arrow::Type::DATE32:
      return copyArrayRowNumeric<arrow::Date32Type>(array, numCopies);
    case arrow::Type::DATE64:
      return copyArrayRowNumeric<arrow::Date64Type>(array, numCopies);
    case arrow::Type::STRING:
      return copyArrayRowString(std::static_pointer_cast<arrow::StringArray>(array), numCopies);
    default: return tl::make_unexpected(
              fmt::format("Unsupported type to copy array rows, '{}'", array->type()->ToString()));
  }
}

tl::expected<std::shared_ptr<arrow::StringArray>, std::string>
Util::copyArrayRowString(const std::shared_ptr<arrow::StringArray> &array, int numCopies) {
  using offsetType = arrow::StringType::offset_type;

  // info of original array
  int64_t length = array->data()->length;   // num elements
  int64_t offset = array->data()->offset;   // offset in data, in num elements
  offsetType* valueOffsets = (offsetType*) array->raw_value_offsets();    // offsets of each individual value
  const uint8_t* data = array->raw_data();
  offsetType bytesData = valueOffsets[offset + length] - valueOffsets[offset];

  // allocate new buffers
  int64_t newBytesValueOffsets = (length * numCopies + 1) * sizeof(offsetType);
  auto expNewValueOffsetsBuffer = arrow::AllocateBuffer(newBytesValueOffsets, arrow::default_memory_pool());
  if (!expNewValueOffsetsBuffer.ok()) {
    return tl::make_unexpected(expNewValueOffsetsBuffer.status().message());
  }
  std::shared_ptr<arrow::Buffer> newValueOffsetsBuffer = std::move(*expNewValueOffsetsBuffer);
  auto expNewDataBuffer = arrow::AllocateBuffer(bytesData * numCopies, arrow::default_memory_pool());
  if (!expNewDataBuffer.ok()) {
    return tl::make_unexpected(expNewDataBuffer.status().message());
  }
  std::shared_ptr<arrow::Buffer> newDataBuffer = std::move(*expNewDataBuffer);

  // assign values to the new buffers
  offsetType* newOffsetsData = (offsetType*) newValueOffsetsBuffer->mutable_data();
  uint8_t* newData = newDataBuffer->mutable_data();
  offsetType writePos = 0;
  for (int i = 0; i < length; ++i) {
    offsetType srcPos = valueOffsets[offset + i];
    offsetType writeLen = valueOffsets[offset + i + 1] - srcPos;
    for (int j = 0; j < numCopies; ++j) {
      *newOffsetsData++ = writePos;
      memcpy(newData + writePos, data + srcPos, writeLen);
      writePos += writeLen;
    }
  }
  *newOffsetsData = writePos;   // write last offset

  // make out array
  return std::make_shared<arrow::StringArray>(length * numCopies, newValueOffsetsBuffer, newDataBuffer);
}

double Util::getFixLen(const std::shared_ptr<arrow::Schema> &schema) {
  double fixLen = 0;
  for (const auto &field: schema->fields()) {
    fixLen += getFixLen(field->type());
  }
  return fixLen;
}

double Util::getFixLen(const std::shared_ptr<arrow::DataType> &type) {
  switch (type->id()) {
    case arrow::Type::INT32:
    case arrow::Type::INT64:
    case arrow::Type::DOUBLE:
    case arrow::Type::DATE32:
    case arrow::Type::DATE64:
    case arrow::Type::BOOL: {
      return std::static_pointer_cast<arrow::FixedWidthType>(type)->bit_width() / 8.0;
    }
    default: {
      return 0;
    }
  }
}
}
