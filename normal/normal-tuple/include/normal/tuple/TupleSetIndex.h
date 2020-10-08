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
  TupleSetIndex(std::string columnName, size_t columnIndex, std::shared_ptr<::arrow::Table> table);
  virtual ~TupleSetIndex() = default;

  std::shared_ptr<::arrow::DataType> type();
  int64_t size();
  std::vector<std::shared_ptr<::arrow::ChunkedArray>> columns();
  [[nodiscard]] const std::shared_ptr<::arrow::Table> &getTable() const;

  /**
   * Invokes combinechunks on the underlying value->row map
   *
   * @return
   */
  [[nodiscard]] tl::expected<void, std::string> combine();

  /**
   * Adds the given table to the index
   *
   * @param table
   * @return
   */
  [[nodiscard]] virtual tl::expected<void, std::string> put(const std::shared_ptr<::arrow::Table> &table) = 0;

  /**
   * Adds another index to this index
   *
   * @param other
   * @return
   */
  [[nodiscard]] virtual tl::expected<void, std::string> merge(const std::shared_ptr<TupleSetIndex> &other) = 0;
  virtual std::string toString() = 0;

  [[nodiscard]] virtual tl::expected<void, std::string> validate() = 0;

protected:
  std::string columnName_;
  size_t columnIndex_;
  std::shared_ptr<::arrow::Table> table_;

};

}

#endif //NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_TUPLESETINDEX_H
