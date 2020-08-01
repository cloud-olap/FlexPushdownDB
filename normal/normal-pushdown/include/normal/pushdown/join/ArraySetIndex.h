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

  virtual tl::expected<void, std::string> put(const std::shared_ptr<::arrow::Table> &table) = 0;

  int64_t size(){
    return table_->num_rows();
  }

  void clear(){
    table_ = nullptr;
  }

protected:
  std::shared_ptr<::arrow::Table> table_;
  size_t arrayPos_;

};





#endif //NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_JOIN_ARRAYSETINDEX_H
