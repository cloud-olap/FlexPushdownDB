//
// Created by matt on 1/5/20.
//

#ifndef NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_COLUMNITERATOR_H
#define NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_COLUMNITERATOR_H

#include <memory>

#include <arrow/table.h>
#include <arrow/array.h>

#include "Scalar.h"
#include "ColumnIndex.h"

namespace normal::tuple {

class ColumnIterator {

public:

  /**
   * 5 aliases required for forward iterators
   */
  using iterator_category = std::forward_iterator_tag;
  using value_type = std::shared_ptr<Scalar>;
  using difference_type = ColumnIterator;
  using pointer = std::shared_ptr<value_type>;
  using reference = value_type &;
  using this_type = ColumnIterator;

  /**
   * Default constructor required for forward iterators
   */
  ColumnIterator() :
	  index_(ColumnIndex(0, 0)) {};

  /**
   * Constructs the iterator over the underlying chunked array
   *
   * @param chunkedArray
   * @param chunk
   * @param chunkIndex
   */
  ColumnIterator(std::shared_ptr<::arrow::ChunkedArray> chunkedArray, int chunk, long chunkIndex) :
	  chunkedArray_(std::move(chunkedArray)), index_(ColumnIndex(chunk, chunkIndex)) {}

  /**
   * Advances the iterator
   */
  void advance() {
	if (index_.getChunk() < chunkedArray_->chunk(index_.getChunk())->length()) {
	  index_.setChunkIndex(index_.getChunkIndex() + 1);
	} else {
	  if (index_.getChunk() < chunkedArray_->num_chunks()) {
		index_.setChunk(index_.getChunk() + 1);
	  }
	}
  }

  /**
   * Pre increment operator (++iterator) required for forward iterators
   *
   * @return
   */
  this_type &operator++() {
	advance();
	return *this;
  }

  /**
   * Post increment operator (iterator++) required for forward iterators
   *
   * @return
   */
  this_type operator++(int) {
	auto iterator = *this;
	++(*this);
	return iterator;
  }

  /**
   * Returns the scalar the iterator is pointing at
   *
   * @return
   */
  [[nodiscard]] value_type value() const {
	return getScalar();
  }

  /**
   * Dereference operator required for forward iterators. Need to return value as can't return reference to Scalar
   * wrapper.
   *
   * @return
   */
  value_type operator*() const {
	return getScalar();
  }

  /**
   * Returns the value pointed at
   *
   * @return
   */
  pointer operator->() const {
	return std::make_shared<value_type>(getScalar());
  }

  /**
   * Equality operator required for forward iterators
   *
   * @return
   */
  bool operator==(const this_type &other) {
	return index_.getChunk() == other.index_.getChunk() && index_.getChunkIndex() == other.index_.getChunkIndex();
  }

  /**
   * Not-Equality operator required for forward iterators
   *
   * @return
   */
  bool operator!=(const this_type &other) {
	return !(*this == other);
  }

  /**
   * Difference operator, TODO: Not sure if this really is needed?
   */
  this_type operator-(const difference_type &other) {
	return ColumnIterator(chunkedArray_, index_.getChunk() - other.index_.getChunk(),
						  index_.getChunkIndex() - other.index_.getChunkIndex());
  }

private:

  std::shared_ptr<::arrow::ChunkedArray> chunkedArray_;
  ColumnIndex index_;

  /**
   * Gets the value pointed at by the iterator as an arrow scalar
   *
   * @return
   */
  [[nodiscard]] std::shared_ptr<::arrow::Scalar> getArrowScalar() const {

    // Need to cast to the array type to be able to use the element accessors
	if (chunkedArray_->type()->id() == arrow::int64()->id()) {
	  auto typedArray = std::static_pointer_cast<arrow::Int64Array>(chunkedArray_->chunk(index_.getChunk()));
	  auto value = typedArray->Value(index_.getChunkIndex());
	  auto arrowScalar = arrow::MakeScalar(value);
	  return arrowScalar;
	} else {
	  throw std::runtime_error(
		  "Iterator on column type '" + chunkedArray_->type()->ToString() + "' not implemented yet");
	}
  }

  /**
   * Gets the value pointed at by the iterator as a scalar
   *
   * @return
   */
  [[nodiscard]] value_type getScalar() const {
	return Scalar::make(getArrowScalar());
  }
};

}

#endif //NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_COLUMNITERATOR_H
