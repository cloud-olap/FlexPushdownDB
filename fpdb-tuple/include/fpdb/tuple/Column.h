//
// Created by matt on 1/5/20.
//

#ifndef FPDB_FPDB_TUPLE_INCLUDE_FPDB_TUPLE_COLUMN_H
#define FPDB_FPDB_TUPLE_INCLUDE_FPDB_TUPLE_COLUMN_H

#include <fpdb/tuple/Scalar.h>
#include <fpdb/tuple/ColumnIterator.h>
#include <fpdb/tuple/serialization/ArrowSerializer.h>
#include <fpdb/caf/CAFUtil.h>
#include <arrow/api.h>
#include <arrow/table.h>
#include <arrow/array.h>
#include <tl/expected.hpp>
#include <utility>

namespace fpdb::tuple {

/**
 * A named array of data
 */
class Column {

public:
  explicit Column(std::string name, std::shared_ptr<::arrow::ChunkedArray> array);
  Column() = default;
  Column(const Column&) = default;
  Column& operator=(const Column&) = default;

  static std::shared_ptr<Column> make(const std::string &name, const std::shared_ptr<::arrow::Array> &array);

  static std::shared_ptr<Column> make(const std::string &name, const std::shared_ptr<::arrow::ChunkedArray> &array);

  static std::shared_ptr<Column> make(const std::string &name, const ::arrow::ArrayVector &arrays);

  /**
   * Makes an empty column of the given type
   */
  static std::shared_ptr<Column> make(const std::string &name, const std::shared_ptr<::arrow::DataType> &type);

  static std::vector<std::shared_ptr<::arrow::ChunkedArray>>
  columnVectorToArrowChunkedArrayVector(const std::vector<std::shared_ptr<Column>> &columns);

  [[nodiscard]] const std::string &getName() const;
  void setName(const std::string &Name);
  std::shared_ptr<::arrow::DataType> type();

  long numRows();
  size_t size();

  std::string showString();
  [[nodiscard]] std::string toString() const;

  /**
   * Returns the element in the column at the given row index
   *
   * @param row
   * @return
   */
  tl::expected<std::shared_ptr<Scalar>, std::string> element(long index);

  ColumnIterator begin();

  ColumnIterator end();

  [[nodiscard]] const std::shared_ptr<::arrow::ChunkedArray> &getArrowArray() const;

private:
  std::string name_;
  std::shared_ptr<::arrow::ChunkedArray> array_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, Column& column) {
    auto toBytes = [&column]() -> decltype(auto) {
      return ArrowSerializer::chunkedArray_to_bytes(column.array_);
    };
    auto fromBytes = [&column](const std::vector<std::uint8_t> &bytes) {
      column.array_ = ArrowSerializer::bytes_to_chunkedArray(bytes);
      return true;
    };
    return f.object(column).fields(f.field("name", column.name_),
                                   f.field("table", toBytes, fromBytes));
  }
};

}

using ColumnPtr = std::shared_ptr<fpdb::tuple::Column>;

CAF_BEGIN_TYPE_ID_BLOCK(Column, fpdb::caf::CAFUtil::Column_first_custom_type_id)
CAF_ADD_TYPE_ID(Column, (fpdb::tuple::Column))
CAF_END_TYPE_ID_BLOCK(Column)

namespace caf {
template <>
struct inspector_access<ColumnPtr> : variant_inspector_access<ColumnPtr> {
  // nop
};
} // namespace caf

#endif //FPDB_FPDB_TUPLE_INCLUDE_FPDB_TUPLE_COLUMN_H
