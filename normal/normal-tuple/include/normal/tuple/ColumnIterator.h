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
  ColumnIterator();

  /**
   * Constructs the iterator over the underlying chunked array
   *
   * @param chunkedArray
   * @param chunk
   * @param chunkIndex
   */
  ColumnIterator(std::shared_ptr<::arrow::ChunkedArray> chunkedArray, int chunk, long chunkIndex);

  /**
   * Advances the iterator
   */
  void advance();

  /**
   * Pre increment operator (++iterator) required for forward iterators
   *
   * @return
   */
  this_type &operator++();

  /**
   * Post increment operator (iterator++) required for forward iterators
   *
   * @return
   */
  this_type operator++(int);

  /**
   * Returns the scalar the iterator is pointing at
   *
   * @return
   */
  [[nodiscard]] value_type value() const;

  /**
   * Dereference operator required for forward iterators. Need to return value as can't return reference to Scalar
   * wrapper.
   *
   * @return
   */
  value_type operator*() const;

  /**
   * Returns the value pointed at
   *
   * @return
   */
  pointer operator->() const;

  /**
   * Equality operator required for forward iterators
   *
   * @return
   */
  bool operator==(const this_type &other) const;

  /**
   * Not-Equality operator required for forward iterators
   *
   * @return
   */
  bool operator!=(const this_type &other) const;

  /**
   * Difference operator, TODO: Not sure if this really is needed?
   */
  this_type operator-(const difference_type &other);

private:

  std::shared_ptr<::arrow::ChunkedArray> chunkedArray_;
  ColumnIndex index_;

  /**
   * Gets the value pointed at by the iterator as an arrow scalar
   *
   * @return
   */
  [[nodiscard]] std::shared_ptr<::arrow::Scalar> getArrowScalar() const;

  /**
   * Gets the value pointed at by the iterator as a scalar
   *
   * @return
   */
  [[nodiscard]] value_type getScalar() const;
};

}

#endif //NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_COLUMNITERATOR_H
