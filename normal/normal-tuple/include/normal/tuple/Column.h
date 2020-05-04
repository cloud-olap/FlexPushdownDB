//
// Created by matt on 1/5/20.
//

#ifndef NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_COLUMN_H
#define NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_COLUMN_H

#include <arrow/table.h>
#include <arrow/array.h>
#include <tl/expected.hpp>
#include <fmt/format.h>
#include <utility>

#include "Scalar.h"
#include "ColumnIterator.h"

namespace normal::tuple {

class Column {

public:
  explicit Column(std::shared_ptr<::arrow::ChunkedArray> array) : array_(std::move(array)) {};

  static std::shared_ptr<Column> make(const std::shared_ptr<::arrow::ChunkedArray>& array) {
	if (array->type()->id() == arrow::int64()->id()) {
	  return std::make_shared<Column>(array);
	} else {
	  throw std::runtime_error(
		  "Column type '" + array->type()->ToString() + "' not implemented yet");
	}
  }

  std::shared_ptr<::arrow::DataType> type() {
	return array_->type();
  }

  long numRows() {
	return array_->length();
  }

  std::string showString() {
	std::string s;
	int chunk = 0;
	for (const auto &arrayChunk: array_->chunks()) {
	  s += fmt::format("Chunk {}\n", chunk);
	  s += arrayChunk->ToString();
	  s += "\n";
	  chunk++;
	}
	return s;
  }

  /**
   * TODO: Implement this... maybe
   *
   * @param row
   * @return
   */
//  tl::expected<std::shared_ptr<Scalar>, std::string> value(long row) {
//
//	if(row > array_->length()){
//	  return tl::make_unexpected("Row '" + std::to_string(row) + "' does not exist");
//	}
//
//	if (array_->type()->id() == arrow::int64()->id()) {
//	  auto typedArray = std::static_pointer_cast<arrow::Int64Array>(array_);
//
//	  auto value = typedArray->Value(row);
//	  auto valueScalar = arrow::MakeScalar(value);
//	  return std::make_shared<Scalar>(valueScalar);
//	} else {
//	  return tl::make_unexpected("Column value accessor for type '" + array_->type()->ToString() + "' not implemented yet");
//	}
//  }


  ColumnIterator begin() {
	return ColumnIterator(array_, 0, 0);
  }

  ColumnIterator end() {
	return ColumnIterator(array_,
							 array_->num_chunks() - 1,
							 array_->chunk(array_->num_chunks() - 1)->length());
  }

private:
  std::shared_ptr<::arrow::ChunkedArray> array_;

};

}

#endif //NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_COLUMN_H
