//
// Created by matt on 1/5/20.
//

#include <fmt/format.h>
#include "fpdb/tuple/Column.h"

using namespace fpdb::tuple;

Column::Column(std::string name, std::shared_ptr<::arrow::ChunkedArray> array) :
	name_(std::move(name)),
	array_(std::move(array)) {
}

std::shared_ptr<Column> Column::make(const std::string &name, const std::shared_ptr<::arrow::Array> &array) {
  return std::make_shared<Column>(name, std::make_shared<::arrow::ChunkedArray>(array));
}

std::shared_ptr<Column> Column::make(const std::string &name, const std::shared_ptr<::arrow::ChunkedArray> &array) {
  return std::make_shared<Column>(name, array);
}

std::shared_ptr<Column> Column::make(const std::string &name, const ::arrow::ArrayVector &arrays){
  return make(name, std::make_shared<::arrow::ChunkedArray>(arrays));
}

std::shared_ptr<Column> Column::make(const std::string &name, const std::shared_ptr<::arrow::DataType> &type) {
  std::vector<std::shared_ptr<::arrow::Array>> arrayVector = {};
  auto chunkedArray = std::make_shared<::arrow::ChunkedArray>(arrayVector, type);
  return std::make_shared<Column>(name, chunkedArray);
}

const std::string &Column::getName() const {
  return name_;
}

std::shared_ptr<::arrow::DataType> Column::type() {
  return array_->type();
}

long Column::numRows() {
  return array_->length();
}

tl::expected<std::shared_ptr<Scalar>, std::string> Column::element(long index) {
  auto expArrowScalar = array_->GetScalar(index);
  if (!expArrowScalar.ok()) {
    return tl::make_unexpected(expArrowScalar.status().message());
  }
  return std::make_shared<Scalar>(*expArrowScalar);
}

const std::shared_ptr<::arrow::ChunkedArray> &Column::getArrowArray() const {
  return array_;
}

std::string Column::showString() {
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

std::string Column::toString() const {
  std::string s;
  s += fmt::format("<column|size:{}>", array_->length());
  return s;
}

ColumnIterator Column::begin() {
  return ColumnIterator(array_, 0, 0);
}

ColumnIterator Column::end() {

  /**
   * Needs to point to just past the last element, or in the case of an empty column,
   * needs to be the same as "begin". We use 0 and 0 for an empty array.
   */

  auto endChunkIndex = array_->length() <= 0 ? 0 : array_->num_chunks();
  auto endChunkEndIndex =  array_->length() <= 0 ? 0 : 0;

  return ColumnIterator(array_,
						endChunkIndex,
						endChunkEndIndex);
}

std::vector<std::shared_ptr<::arrow::ChunkedArray>> Column::columnVectorToArrowChunkedArrayVector(const std::vector<std::shared_ptr<
	Column>> &columns) {
  std::vector<std::shared_ptr<::arrow::ChunkedArray>> chunkedArrays;
  chunkedArrays.reserve(columns.size());
  for (const auto &column: columns) {
	auto array = column->array_;
	chunkedArrays.emplace_back(array);
  }
  return chunkedArrays;
}

void Column::setName(const std::string &Name) {
  name_ = Name;
}

size_t Column::size() {
  size_t size = 0;
  for (auto const &chunk: array_->chunks()) {
    for (auto const &buffer: chunk->data()->buffers) {
      if(buffer)
      	size += buffer->size();
    }
  }
  return size;
}
