//
// Created by matt on 1/8/20.
//

#ifndef NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_JOIN_ARRAYSETINDEX_H
#define NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_JOIN_ARRAYSETINDEX_H

#include <utility>
#include <vector>
#include <unordered_map>
#include <arrow/api.h>
#include <tl/expected.hpp>

class ArraySetIndex {
public:
  ArraySetIndex(size_t ArrayPos,
				std::shared_ptr<::arrow::Table> Table);

  virtual ~ArraySetIndex() = default;

  std::shared_ptr<::arrow::DataType> type(){
    return table_->column(arrayPos_)->type();
  }

  virtual tl::expected<void, std::string> put(const std::shared_ptr<::arrow::Table> &table) = 0;

  virtual tl::expected<void, std::string> merge(const std::shared_ptr<ArraySetIndex> &other) = 0;

  int64_t size(){
    return table_->num_rows();
  }

  void clear(){
    table_ = nullptr;
  }

  std::vector<std::shared_ptr<::arrow::ChunkedArray>> columns(){
    return table_->columns();
  }

  [[nodiscard]] const std::shared_ptr<::arrow::Table> &getTable() const;

  virtual std::string toString() = 0;

protected:
  size_t arrayPos_;

  std::shared_ptr<::arrow::Table> table_;
};





#endif //NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_JOIN_ARRAYSETINDEX_H
