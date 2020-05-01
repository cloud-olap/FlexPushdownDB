//
// Created by matt on 1/5/20.
//

#include "Column.h"

#include <utility>

#include <arrow/array.h>
#include <arrow/scalar.h>
#include <spdlog/fmt/bundled/format.h>

using namespace normal::tuple;

Column::Column(std::shared_ptr<::arrow::ChunkedArray> array) : array_(std::move(array)) {}

std::shared_ptr<::arrow::DataType> Column::type() {
  return array_->type();
}

long Column::numRows() {
  return array_->length();
}

std::string Column::showString() {
  std::string s;
  int chunk = 0;
  for(const auto& arrayChunk: array_->chunks()){
    s += fmt::format("Chunk {}\n", chunk);
    s += arrayChunk->ToString();
	s += "\n";
	chunk++;
  }
  return s;
}

//tl::expected<std::shared_ptr<Scalar>, std::string> Column::value(long row) {
//
//  if(row > array_->length()){
//    return tl::make_unexpected("Row '" + std::to_string(row) + "' does not exist");
//  }
//
//  if (array_->type()->id() == arrow::int64()->id()) {
//    auto typedArray = std::static_pointer_cast<arrow::Int64Array>(array_);
//
//    auto value = typedArray->Value(row);
//    auto valueScalar = arrow::MakeScalar(value);
//    return std::make_shared<Scalar>(valueScalar);
//  } else {
//    return tl::make_unexpected("Column value accessor for type '" + array_->type()->ToString() + "' not implemented yet");
//  }
//}
