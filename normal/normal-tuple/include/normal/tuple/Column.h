//
// Created by matt on 1/5/20.
//

#ifndef NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_COLUMN_H
#define NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_COLUMN_H

#include <arrow/api.h>
#include <arrow/table.h>
#include <arrow/array.h>
#include <tl/expected.hpp>
#include <fmt/format.h>
#include <utility>

#include "Scalar.h"
#include "ColumnIterator.h"

namespace normal::tuple {

/**
 * A named array of data
 */
class Column {

public:
  explicit Column(std::string name, std::shared_ptr<::arrow::ChunkedArray> array) :
	  name_(std::move(name)),
	  array_(std::move(array)) {

	if (array_->type()->id() == arrow::Int64Type::type_id) {
	  builder_ = std::make_shared<::arrow::Int64Builder>();
	}
  };

  static std::shared_ptr<Column> make(const std::string &name, const std::shared_ptr<::arrow::ChunkedArray> &array) {
	return std::make_shared<Column>(name, array);
  }

  /**
   * Makes an empty column of the given type
   */
  static std::shared_ptr<Column> make(const std::string &name, const std::shared_ptr<::arrow::DataType> &type) {
	std::vector<std::shared_ptr<::arrow::Array>> arrayVector = {};
	auto chunkedArray = std::make_shared<::arrow::ChunkedArray>(arrayVector, type);
	return std::make_shared<Column>(name, chunkedArray);
  }

  static std::vector<std::shared_ptr<::arrow::ChunkedArray>> columnVectorToArrowChunkedArrayVector(const std::vector<std::shared_ptr<
	  Column>> &columns) {
	std::vector<std::shared_ptr<::arrow::ChunkedArray>> chunkedArrays;
	chunkedArrays.reserve(columns.size());
	for (const auto &column: columns) {
	  auto array = column->array_;
	  chunkedArrays.emplace_back(array);
	}
	return chunkedArrays;
  }

  [[nodiscard]] const std::string &getName() const {
	return name_;
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
   * Returns the element in the column at the given row index
   *
   * @param row
   * @return
   */
  tl::expected<std::shared_ptr<Scalar>, std::string> element(long index) {

	if (index > array_->length()) {
	  return tl::make_unexpected("Row '" + std::to_string(index) + "' does not exist");
	}

	if (array_->type()->id() == arrow::Int32Type::type_id) {
	  auto typedArray = std::static_pointer_cast<arrow::Int32Array>(array_->Slice(index)->chunk(0));
	  auto value = typedArray->Value(0);
	  auto valueScalar = arrow::MakeScalar(value);
	  return std::make_shared<Scalar>(valueScalar);
	} else if (array_->type()->id() == arrow::Int64Type::type_id) {
	  auto typedArray = std::static_pointer_cast<arrow::Int64Array>(array_->Slice(index)->chunk(0));
	  auto value = typedArray->Value(0);
	  auto valueScalar = arrow::MakeScalar(value);
	  return std::make_shared<Scalar>(valueScalar);
	} else if (array_->type()->id() == arrow::FloatType::type_id) {
	  auto typedArray = std::static_pointer_cast<arrow::FloatArray>(array_->Slice(index)->chunk(0));
	  auto value = typedArray->Value(0);
	  auto valueScalar = arrow::MakeScalar(value);
	  return std::make_shared<Scalar>(valueScalar);
	} else if (array_->type()->id() == arrow::DoubleType::type_id) {
	  auto typedArray = std::static_pointer_cast<arrow::DoubleArray>(array_->Slice(index)->chunk(0));
	  auto value = typedArray->Value(0);
	  auto valueScalar = arrow::MakeScalar(value);
	  return std::make_shared<Scalar>(valueScalar);
	} else if (array_->type()->id() == arrow::BooleanType::type_id) {
	  auto typedArray = std::static_pointer_cast<arrow::BooleanArray>(array_->Slice(index)->chunk(0));
	  auto value = typedArray->Value(0);
	  auto valueScalar = arrow::MakeScalar(value);
	  return std::make_shared<Scalar>(valueScalar);
	} else if (array_->type()->id() == arrow::StringType::type_id) {
	  auto typedArray = std::static_pointer_cast<arrow::StringArray>(array_->Slice(index)->chunk(0));
	  auto value = typedArray->GetString(0);
	  auto valueScalar = arrow::MakeScalar(value);
	  return std::make_shared<Scalar>(valueScalar);
	} else if (array_->type()->id() == arrow::Decimal128Type::type_id) {
	  auto typedArray = std::static_pointer_cast<arrow::Decimal128Array>(array_->Slice(index)->chunk(0));
	  auto value = typedArray->Value(0);
	  auto decimalValue = static_cast<::arrow::Decimal128>(value);
	  auto valueScalar = std::make_shared<::arrow::Decimal128Scalar>(decimalValue, typedArray->type());
	  return std::make_shared<Scalar>(valueScalar);
	} else {
	  return tl::make_unexpected(
		  "Column value accessor for type '" + array_->type()->ToString() + "' not implemented yet");
	}
  }

  ColumnIterator begin() {
	return ColumnIterator(array_, 0, 0);
  }

  ColumnIterator end() {
	return ColumnIterator(array_,
						  array_->num_chunks() - 1,
						  array_->chunk(array_->num_chunks() - 1)->length());
  }

  const std::shared_ptr<::arrow::ChunkedArray> &getArrowArray() const {
	return array_;
  }

private:
  std::string name_;
  std::shared_ptr<::arrow::ChunkedArray> array_;
  std::shared_ptr<::arrow::ArrayBuilder> builder_;

};

}

#endif //NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_COLUMN_H
