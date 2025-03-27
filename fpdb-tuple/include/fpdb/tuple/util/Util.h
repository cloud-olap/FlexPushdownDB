//
// Created by Yifei Yang on 1/18/22.
//

#ifndef FPDB_FPDB_TUPLE_INCLUDE_FPDB_TUPLE_UTIL_UTIL_H
#define FPDB_FPDB_TUPLE_INCLUDE_FPDB_TUPLE_UTIL_UTIL_H

#include <arrow/api.h>
#include <tl/expected.hpp>

namespace fpdb::tuple::util {

class Util {

public:
  static tl::expected<std::shared_ptr<arrow::Array>, std::string>
  makeEmptyArray(const std::shared_ptr<arrow::DataType> &type);

  static tl::expected<std::shared_ptr<arrow::RecordBatch>, std::string>
  makeEmptyRecordBatch(const std::shared_ptr<arrow::Schema> &schema);

  /**
   * Make record batches from table, generating at least 1 batch
   */
  static tl::expected<::arrow::RecordBatchVector, std::string>
  table_to_record_batches(const std::shared_ptr<::arrow::Table> &table);

  /**
   * Get the size (num bytes)
   */
  static int64_t getSize(const std::shared_ptr<arrow::RecordBatch> &recordBatch);

  /**
   * Check if the table has the same schema to targetSchema (field order can be different),
   * if so, calibrate the table into targeSchema by rearranging columns
   */
  static tl::expected<std::shared_ptr<arrow::Table>, std::string> calibrateSchema(
          const std::shared_ptr<arrow::Table> &table, const std::shared_ptr<arrow::Schema> &targetSchema);

  /**
   * Copy entire "array" "numCopies" times into a new array
   * e.g., "array" = [1,2,3,4], "numCopies" = 3, return [1,2,3,4,1,2,3,4,1,2,3,4]
   */
  static tl::expected<std::shared_ptr<arrow::Array>, std::string>
  copyArrayWhole(const std::shared_ptr<arrow::Array> &array, int numCopies);

  /**
   * Copy each row of "array" "numCopies" times into a new array
   * e.g., "array" = [1,2,3,4], "numCopies" = 3, return [1,1,1,2,2,2,3,3,3,4,4,4]
   */
  static tl::expected<std::shared_ptr<arrow::Array>, std::string>
  copyArrayRow(const std::shared_ptr<arrow::Array> &array, int numCopies);

  /**
   * Get the total length of fix-len fields
   */
  static double getFixLen(const std::shared_ptr<arrow::Schema> &schema);

  /**
   * Get the length for fix-len types, return 0 for var-len types
   */
  static double getFixLen(const std::shared_ptr<arrow::DataType> &type);

private:
  /**
   * Impl. of "copyArrayWhole()"
   */
  template <typename ArrowType>
  static tl::expected<std::shared_ptr<arrow::Array>, std::string>
  copyArrayWholeNumeric(const std::shared_ptr<arrow::Array> &array, int numCopies) {
    using valueType = typename ArrowType::c_type;
    using ArrayType = typename arrow::TypeTraits<ArrowType>::ArrayType;

    // info of original array
    int64_t length = array->data()->length;   // num elements
    int64_t offset = array->data()->offset;   // offset in data, in num elements
    const uint8_t* data = array->data()->buffers[1]->data();

    // get size (in bytes) of data and offset
    int64_t bytesData = length * sizeof(valueType);
    int64_t bytesOffset = offset * sizeof(valueType);

    // allocate new buffer for copy
    auto expNewBuffer = arrow::AllocateBuffer(bytesData * numCopies, arrow::default_memory_pool());
    if (!expNewBuffer.ok()) {
      return tl::make_unexpected(expNewBuffer.status().message());
    }
    std::shared_ptr<arrow::Buffer> newBuffer = std::move(*expNewBuffer);

    // copy data into new buffer, and make array data
    int64_t currBytesOffset = 0;
    for (int i = 0; i < numCopies; ++i) {
      memcpy(newBuffer->mutable_data() + currBytesOffset, data + bytesOffset, bytesData);
      currBytesOffset += bytesData;
    }
    auto newArrayData = arrow::ArrayData::Make(array->type(), length * numCopies, {nullptr, newBuffer}, 0);

    // make out array
    return std::make_shared<ArrayType>(newArrayData);
  }

  static tl::expected<std::shared_ptr<arrow::StringArray>, std::string>
  copyArrayWholeString(const std::shared_ptr<arrow::StringArray> &array, int numCopies);

  /**
   * Impl. of "copyArrayRow()"
   */
  template <typename ArrowType>
  static tl::expected<std::shared_ptr<arrow::Array>, std::string>
  copyArrayRowNumeric(const std::shared_ptr<arrow::Array> &array, int numCopies) {
    using valueType = typename ArrowType::c_type;
    using ArrayType = typename arrow::TypeTraits<ArrowType>::ArrayType;

    // info of original array
    int64_t length = array->data()->length;   // num elements
    int64_t offset = array->data()->offset;   // offset in data, in num elements
    const uint8_t* data = array->data()->buffers[1]->data();

    // allocate new buffer for copy
    int64_t bytesData = length * sizeof(valueType);
    auto expNewBuffer = arrow::AllocateBuffer(bytesData * numCopies, arrow::default_memory_pool());
    if (!expNewBuffer.ok()) {
      return tl::make_unexpected(expNewBuffer.status().message());
    }
    std::shared_ptr<arrow::Buffer> newBuffer = std::move(*expNewBuffer);

    // assign values to new buffer, and make array data
    valueType* newData = (valueType*) newBuffer->mutable_data();
    for (int i = 0; i < length; ++i) {
      valueType value = ((valueType*) data)[offset + i];
      for (int j = 0; j < numCopies; ++j) {
        newData[j] = value;
      }
      newData += numCopies;
    }
    auto newArrayData = arrow::ArrayData::Make(array->type(), length * numCopies, {nullptr, newBuffer}, 0);

    // make out array
    return std::make_shared<ArrayType>(newArrayData);
  }

  static tl::expected<std::shared_ptr<arrow::StringArray>, std::string>
  copyArrayRowString(const std::shared_ptr<arrow::StringArray> &array, int numCopies);
};

}


#endif //FPDB_FPDB_TUPLE_INCLUDE_FPDB_TUPLE_UTIL_UTIL_H
