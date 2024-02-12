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

  if (index > array_->length()) {
	  return tl::make_unexpected("Row '" + std::to_string(index) + "' does not exist");
  }

  if (array_->type()->id() == arrow::Int32Type::type_id) {
    auto typedArray = std::static_pointer_cast<arrow::Int32Array>(array_->Slice(index)->chunk(0));
    auto value = typedArray->Value(0);
    auto valueScalar = arrow::MakeScalar(value);
    return std::make_shared<Scalar>(valueScalar);
  }
  else if (array_->type()->id() == arrow::Int64Type::type_id) {
    auto typedArray = std::static_pointer_cast<arrow::Int64Array>(array_->Slice(index)->chunk(0));
    auto value = typedArray->Value(0);
    auto valueScalar = arrow::MakeScalar(value);
    return std::make_shared<Scalar>(valueScalar);
  }
  else if (array_->type()->id() == arrow::FloatType::type_id) {
    auto typedArray = std::static_pointer_cast<arrow::FloatArray>(array_->Slice(index)->chunk(0));
    auto value = typedArray->Value(0);
    auto valueScalar = arrow::MakeScalar(value);
    return std::make_shared<Scalar>(valueScalar);
  }
  else if (array_->type()->id() == arrow::DoubleType::type_id) {
    auto typedArray = std::static_pointer_cast<arrow::DoubleArray>(array_->Slice(index)->chunk(0));
    auto value = typedArray->Value(0);
    auto valueScalar = arrow::MakeScalar(value);
    return std::make_shared<Scalar>(valueScalar);
  }
  else if (array_->type()->id() == arrow::BooleanType::type_id) {
    auto typedArray = std::static_pointer_cast<arrow::BooleanArray>(array_->Slice(index)->chunk(0));
    auto value = typedArray->Value(0);
    auto valueScalar = arrow::MakeScalar(value);
    return std::make_shared<Scalar>(valueScalar);
  }
  else if (array_->type()->id() == arrow::StringType::type_id) {
    auto typedArray = std::static_pointer_cast<arrow::StringArray>(array_->Slice(index)->chunk(0));
    auto value = typedArray->GetString(0);
    auto valueScalar = arrow::MakeScalar(value);
    return std::make_shared<Scalar>(valueScalar);
  }
  else if (array_->type()->id() == arrow::Date64Type::type_id) {
    auto typedArray = std::static_pointer_cast<arrow::Date64Array>(array_->Slice(index)->chunk(0));
    auto value = typedArray->Value(0);
    auto valueScalar = arrow::MakeScalar(arrow::date64(), value).ValueOrDie();
    return std::make_shared<Scalar>(valueScalar);
  }
  else if (array_->type()->id() == arrow::Decimal128Type::type_id) {
    auto typedArray = std::static_pointer_cast<arrow::Decimal128Array>(array_->Slice(index)->chunk(0));
    auto value = typedArray->Value(0);
    auto decimalValue = static_cast<::arrow::Decimal128>(value);
    auto valueScalar = std::make_shared<::arrow::Decimal128Scalar>(decimalValue, typedArray->type());
    return std::make_shared<Scalar>(valueScalar);
  }
  else {
    return tl::make_unexpected(
      "Column value accessor for type '" + array_->type()->ToString() + "' not implemented yet");
  }
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
