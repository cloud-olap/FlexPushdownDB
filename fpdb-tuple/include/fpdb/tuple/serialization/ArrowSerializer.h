//
// Created by Yifei Yang on 1/12/22.
//

#ifndef FPDB_FPDB_TUPLE_INCLUDE_FPDB_TUPLE_SERIALIZATION_ARROWSERIALIZER_H
#define FPDB_FPDB_TUPLE_INCLUDE_FPDB_TUPLE_SERIALIZATION_ARROWSERIALIZER_H

#include <caf/all.hpp>
#include <arrow/api.h>
#include <tl/expected.hpp>

namespace fpdb::tuple {

class ArrowSerializer {

public:
  /**
   * Serialization and deserialization methods of arrow::Table.
   * If setting copy_view = false, need to make sure the bytes vector doesn't become invalid
   * during the usage of the output table.
   */
  static std::shared_ptr<arrow::Table> bytes_to_table(const std::vector<std::uint8_t>& bytes_vec,
                                                      bool copy_view = true);
  static std::vector<std::uint8_t> table_to_bytes(const std::shared_ptr<arrow::Table>& table);

  /**
   * This is just used by the temp fix for the issue that arrow's group-by kernel will crash
   * when using unaligned buffers from flight.
   */
  static std::shared_ptr<arrow::Table> align_table_by_copy(const std::shared_ptr<arrow::Table>& table);

  /**
   * Serialization and deserialization methods of arrow::RecordBatch.
   */
  static std::shared_ptr<arrow::RecordBatch> bytes_to_recordBatch(const std::vector<std::uint8_t>& bytes_vec);
  static std::vector<std::uint8_t> recordBatch_to_bytes(const std::shared_ptr<arrow::RecordBatch>& recordBatch);

  /**
   * Serialization and deserialization methods of arrow::Schema.
   */
  static std::shared_ptr<arrow::Schema> bytes_to_schema(const std::vector<std::uint8_t>& bytes_vec);
  static std::vector<std::uint8_t> schema_to_bytes(const std::shared_ptr<arrow::Schema>& schema);

  /**
   * Serialization and deserialization methods of arrow::ChunkedArray.
   * As the primitive serialization unit of Arrow is RecordBatch, here we do indirectly by making a Table first
   */
  static std::shared_ptr<arrow::ChunkedArray> bytes_to_chunkedArray(const std::vector<std::uint8_t>& bytes_vec);
  static std::vector<std::uint8_t> chunkedArray_to_bytes(const std::shared_ptr<arrow::ChunkedArray>& chunkedArray);

  /**
   * Serialization and deserialization methods of arrow::Scalar
   * Do this by converting the scalar to a record batch
   */
  static std::shared_ptr<arrow::Scalar> bytes_to_scalar(const std::vector<std::uint8_t>& bytes_vec);
  static std::vector<std::uint8_t> scalar_to_bytes(const std::shared_ptr<arrow::Scalar>& scalar);

  /**
   * Serialization and deserialization methods of arrow::DataType
   * Do this in a straightforward way (using name()) instead of using schema serialization
   */
  static std::shared_ptr<arrow::DataType> bytes_to_dataType(const std::vector<std::uint8_t>& bytes_vec);
  static std::vector<std::uint8_t> dataType_to_bytes(const std::shared_ptr<arrow::DataType>& dataType);

  /**
   * Transformation between bitmap and recordBatch
   */
  static constexpr std::string_view BITMAP_FIELD_NAME = "bitmap";
  static tl::expected<std::shared_ptr<arrow::RecordBatch>, std::string>
  bitmap_to_recordBatch(const std::vector<int64_t> &bitmap);
  static tl::expected<std::vector<int64_t>, std::string>
  recordBatch_to_bitmap(const std::shared_ptr<arrow::RecordBatch> &recordBatch);

};

}


#endif //FPDB_FPDB_TUPLE_INCLUDE_FPDB_TUPLE_SERIALIZATION_ARROWSERIALIZER_H
