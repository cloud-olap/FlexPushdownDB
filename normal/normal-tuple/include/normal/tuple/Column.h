//
// Created by matt on 1/5/20.
//

#ifndef NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_COLUMN_H
#define NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_COLUMN_H

#include <arrow/table.h>
#include <arrow/array.h>
#include <tl/expected.hpp>
#include <utility>

#include "Scalar.h"

namespace normal::tuple {

class Column {

public:
  explicit Column(std::shared_ptr<::arrow::ChunkedArray> array);

  std::shared_ptr<::arrow::DataType> type();
  long numRows();
  std::string showString();
//  tl::expected<std::shared_ptr<Scalar>, std::string> value(long row);

  class iterator {
  public:
	typedef iterator self_type;
	typedef Scalar value_type;
	typedef Scalar &reference;
	typedef std::shared_ptr<Scalar> pointer;
	typedef std::forward_iterator_tag iterator_category;
	typedef int difference_type;
	iterator(std::shared_ptr<::arrow::ChunkedArray> chunkedArray, int chunk, long index) : chunkedArray_(std::move(
		chunkedArray)), chunk_(chunk), index_(index) {}
	void next() {
	  if (index_ < chunkedArray_->chunk(chunk_)->length()) {
		index_++;
	  } else {
		if (chunk_ < chunkedArray_->num_chunks()) {
		  chunk_++;
		}
	  }
	}
	self_type operator++() {
	  self_type i = *this;
	  next();
	  return i;
	}
	self_type operator++(int junk) {
	  next();
	  return *this;
	}
	std::shared_ptr<Scalar> get() {
	  if (chunkedArray_->type()->id() == arrow::int64()->id()) {
		auto typedArray = std::static_pointer_cast<arrow::Int64Array>(chunkedArray_->chunk(chunk_));
		auto value = typedArray->Value(index_);
		auto valueScalar = arrow::MakeScalar(value);
		return std::make_shared<Scalar>(valueScalar);
	  } else {
		throw std::runtime_error(
			"Iterator on column type '" + chunkedArray_->type()->ToString() + "' not implemented yet");
	  }
	}
	reference operator*() { return *get(); }
	pointer operator->() { return get(); }
	bool operator==(const self_type &rhs) { return chunk_ == rhs.chunk_ && index_ == rhs.index_; }
	bool operator!=(const self_type &rhs) { return chunk_ != rhs.chunk_ || index_ != rhs.index_; }
  private:
	std::shared_ptr<::arrow::ChunkedArray> chunkedArray_;
	int chunk_;
	long index_;
  };

  class const_iterator {
  public:
	typedef const_iterator self_type;
	typedef Scalar value_type;
	typedef Scalar &reference;
	typedef std::shared_ptr<Scalar> pointer;
	typedef std::forward_iterator_tag iterator_category;
	typedef int difference_type;
	const_iterator(std::shared_ptr<::arrow::ChunkedArray> chunkedArray, int chunk, long index)
		: chunkedArray_(std::move(
		chunkedArray)), chunk_(chunk), index_(index) {}
	void next() {
	  if (index_ < chunkedArray_->chunk(chunk_)->length()) {
		index_++;
	  } else {
		if (chunk_ < chunkedArray_->num_chunks()) {
		  chunk_++;
		}
	  }
	}
	self_type operator++() {
	  self_type i = *this;
	  next();
	  return i;
	}
	self_type operator++(int junk) {
	  next();
	  return *this;
	}
	std::shared_ptr<Scalar> value() {
	  if (chunkedArray_->type()->id() == arrow::int64()->id()) {
		auto typedArray = std::static_pointer_cast<arrow::Int64Array>(chunkedArray_->chunk(chunk_));
		auto value = typedArray->Value(index_);
		auto valueScalar = arrow::MakeScalar(value);
		return std::make_shared<Scalar>(valueScalar);
	  } else {
		throw std::runtime_error(
			"Iterator on column type '" + chunkedArray_->type()->ToString() + "' not implemented yet");
	  }
	}
	const reference operator*() { return *value(); }
	const pointer operator->() { return value(); }
	bool operator==(const self_type &rhs) { return chunk_ == rhs.chunk_ && index_ == rhs.index_; }
	bool operator!=(const self_type &rhs) { return chunk_ != rhs.chunk_ || index_ != rhs.index_; }
  private:
	std::shared_ptr<::arrow::ChunkedArray> chunkedArray_;
	int chunk_;
	long index_;
  };

  iterator begin() {
	return iterator(array_,
					0, 0);
  }
  iterator end() {
	return iterator(array_,
					array_->num_chunks() - 1,
					array_->chunk(array_->num_chunks() - 1)->length());
  }

  const_iterator begin() const {
	return const_iterator(array_,
						  0, 0);
  }
  const_iterator end() const {
	return const_iterator(array_,
						  array_->num_chunks() - 1,
						  array_->chunk(array_->num_chunks() - 1)->length());
  }

private:
  std::shared_ptr<::arrow::ChunkedArray> array_;

};

}

#endif //NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_COLUMN_H
