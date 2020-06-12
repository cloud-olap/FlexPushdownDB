//
// Created by matt on 1/5/20.
//

#ifndef NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_COLUMN_H
#define NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_COLUMN_H

#include <arrow/api.h>
#include <arrow/table.h>
#include <arrow/array.h>
#include <tl/expected.hpp>
#include <utility>

#include "Scalar.h"
#include "ColumnIterator.h"

namespace normal::tuple {

/**
 * A named array of data
 */
class Column {

public:
  explicit Column(std::string name, std::shared_ptr<::arrow::ChunkedArray> array);

  static std::shared_ptr<Column> make(const std::string &name, const std::shared_ptr<::arrow::Array> &array);

  static std::shared_ptr<Column> make(const std::string &name, const std::shared_ptr<::arrow::ChunkedArray> &array);

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


};

}

#endif //NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_COLUMN_H
