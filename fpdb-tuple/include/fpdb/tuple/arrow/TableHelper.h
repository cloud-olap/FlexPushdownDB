//
// Created by matt on 9/4/20.
//

#ifndef FPDB_FPDB_CORE_INCLUDE_FPDB_CORE_ARROW_TABLEHELPER_H
#define FPDB_FPDB_CORE_INCLUDE_FPDB_CORE_ARROW_TABLEHELPER_H

#include <tl/expected.hpp>
#include <string>
#include <arrow/api.h>
#include <fpdb/tuple/Globals.h>
#include <fpdb/tuple/ColumnName.h>

#include "fpdb/tuple/arrow/ArrayHelper.h"

/**
 * Bunch of utilities for working with Arrow tables
 *
 */
class TableHelper {
public:

  /**
   * Returns an element from the array using the given index.
   *
   * TODO: It's not clear how to find which chunk the row is in, so we slice out just that row and then
   *  get the first chunk from the slice. It works but is this the best way to get the data out?
   *
   * NOTE: This is really just for testing, its not the most efficient way to get elements.
   *
   * FIXME: This should go in the chunked array helper
   *
   * @tparam ARROW_TYPE
   * @tparam C_TYPE
   * @param array
   * @param row
   * @return
   */
  template<typename ARROW_TYPE, typename C_TYPE = typename ARROW_TYPE::c_type>
  static tl::expected<C_TYPE, std::string> value(arrow::ChunkedArray &array,
												 int index) {

	// Check index
	if (index > array.length())
	  return tl::unexpected("Row '" + std::to_string(index) + "' not found");

	auto slice = array.Slice(index, 1);
	auto sliceChunk = slice->chunk(0);

	using ARROW_ARRAY_TYPE = typename arrow::TypeTraits<ARROW_TYPE>::ArrayType;

	// FIXME: type_singleton is not available for all arrow types. What to use instead?
//    auto arrowType = arrow::TypeTraits<ARROW_TYPE>::type_singleton();

	// Check types
//    if (chunk->type_id() != arrowType->id())
//      return tl::unexpected("Value type '" + chunk->type()->ToString() + "' does not match type template parameter "
//                                + arrowType->ToString());

	auto &typedArray = dynamic_cast<ARROW_ARRAY_TYPE &>(*sliceChunk);
	return ArrayHelper<ARROW_ARRAY_TYPE, C_TYPE>::at(typedArray, 0);
  }

  /**
   * Returns an element from the table given a column number and row number.
   *
   * @tparam ARROW_TYPE
   * @tparam C_TYPE
   * @param columnName
   * @param row
   * @param table
   * @return
   */
  template<typename ARROW_TYPE, typename C_TYPE = typename ARROW_TYPE::c_type>
  static tl::expected<C_TYPE, std::string> value(const std::string &columnName,
												 int row,
												 const arrow::Table &table) {

    auto canonicalColumnName = fpdb::tuple::ColumnName::canonicalize(columnName);

	// Check column name
	auto array = table.GetColumnByName(canonicalColumnName);
	if (!array)
	  return tl::unexpected("Column '" + columnName + "' not found");

	return value<ARROW_TYPE, C_TYPE>(*array, row);
  }

  /**
   * Returns an element from the table given a column number and row number.
   *
   * @tparam ARROW_TYPE
   * @tparam C_TYPE
   * @param col
   * @param row
   * @param table
   * @return
   */
  template<typename ARROW_TYPE, typename C_TYPE = typename ARROW_TYPE::c_type>
  static tl::expected<C_TYPE, std::string> value(int col,
												 int row,
												 const arrow::Table &table) {

	// Check column index
	auto array = table.column(col);
	if (!array)
	  return tl::unexpected("Column '" + std::to_string(col) + "' not found");

	return value<ARROW_TYPE, C_TYPE>(*array, row);
  }

  template<typename ARROW_TYPE, typename C_TYPE = typename ARROW_TYPE::c_type>
  static tl::expected<std::shared_ptr<std::vector<C_TYPE>>, std::string> vector(arrow::ChunkedArray &array) {

    auto vector = std::make_shared<std::vector<C_TYPE>>();

	using ARROW_ARRAY_TYPE = typename arrow::TypeTraits<ARROW_TYPE>::ArrayType;

    for(const auto& chunk: array.chunks()){
	  auto &typedChunk = dynamic_cast<ARROW_ARRAY_TYPE &>(*chunk);
	  for (int i = 0; i < typedChunk.length(); ++i) {
		vector->push_back(typedChunk.Value(i));
	  }
    }

	return vector;
  }
};

#endif //FPDB_FPDB_CORE_INCLUDE_FPDB_CORE_ARROW_TABLEHELPER_H
