//
// Created by matt on 1/8/20.
//

#ifndef NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_TUPLESETINDEX_H
#define NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_TUPLESETINDEX_H

#include <utility>
#include <vector>
#include <unordered_map>
#include <arrow/api.h>
#include <tl/expected.hpp>

namespace normal::tuple {

/**
 * An index into a tuple set
 *
 * Contains a map of values for a particular column to row numbers in the tuple set
 *
 * TODO: Should really operate on tuple sets and not an arrow table
 */
class TupleSetIndex {
public:
  TupleSetIndex(size_t columnIndex, std::shared_ptr<::arrow::Table> table);
  virtual ~TupleSetIndex() = default;

  std::shared_ptr<::arrow::DataType> type();
  int64_t size();
  std::vector<std::shared_ptr<::arrow::ChunkedArray>> columns();
  [[nodiscard]] const std::shared_ptr<::arrow::Table> &getTable() const;

  virtual tl::expected<void, std::string> put(const std::shared_ptr<::arrow::Table> &table) = 0;
  virtual tl::expected<void, std::string> merge(const std::shared_ptr<TupleSetIndex> &other) = 0;
  virtual std::string toString() = 0;

protected:
  size_t columnIndex_;
  std::shared_ptr<::arrow::Table> table_;

};

}

#endif //NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_TUPLESETINDEX_H
