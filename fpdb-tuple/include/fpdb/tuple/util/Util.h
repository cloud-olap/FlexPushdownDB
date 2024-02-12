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

};

}


#endif //FPDB_FPDB_TUPLE_INCLUDE_FPDB_TUPLE_UTIL_UTIL_H
