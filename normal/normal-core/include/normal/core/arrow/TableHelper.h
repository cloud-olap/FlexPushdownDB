//
// Created by matt on 9/4/20.
//

#ifndef NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_ARROW_TABLEHELPER_H
#define NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_ARROW_TABLEHELPER_H

#include <tl/expected.hpp>
#include <string>
#include <arrow/api.h>

#include "normal/core/arrow/ArrayHelper.h"

/**
 * Bunch of utilities for working with Arrow tables
 *
 */
class TableHelper {
public:

  /**
   * Returns an element from the table given a column name and row.
   *
   * TODO: It's not clear how to find which chunk the row is in, so we slice out just that row and then
   * get the first chunk from the slice. It works but is this the best way to get the data out?
   *
   * NOTE: This is really just for testing, its not the most efficient way to get elements.
   *
   * @tparam ARROW_TYPE
   * @tparam C_TYPE
   * @param columnName
   * @param row
   * @return
   */
  template<typename ARROW_TYPE, typename C_TYPE = typename ARROW_TYPE::c_type>
  static tl::expected<C_TYPE, std::string> value(const std::string &columnName,
                                                 int row,
                                                 const arrow::Table &table) {

    // Check column name
    auto column = table.GetColumnByName(columnName);
    if (!column)
      return tl::unexpected("Column '" + columnName + "' not found");

    // Check row
    if (row > table.num_rows())
      return tl::unexpected("Row '" + std::to_string(row) + "' not found");

    auto slice = column->Slice(row, 1);
    auto chunk = column->chunk(0);

    using ARROW_ARRAY_TYPE = typename arrow::TypeTraits<ARROW_TYPE>::ArrayType;
    auto arrowType = arrow::TypeTraits<ARROW_TYPE>::type_singleton();

    // Check types
    if (chunk->type_id() != arrowType->id())
      return tl::unexpected("Value type '" + chunk->type()->ToString() + "' does not match type template parameter "
                                + arrowType->ToString());

    auto &typedArray = dynamic_cast<ARROW_ARRAY_TYPE &>(*chunk);
    return ArrayHelper<ARROW_ARRAY_TYPE, C_TYPE>::at(typedArray, 0);
  }
};

#endif //NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_ARROW_TABLEHELPER_H
