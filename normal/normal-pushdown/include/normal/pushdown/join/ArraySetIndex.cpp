//
// Created by matt on 1/8/20.
//

#include "ArraySetIndex.h"

#include <utility>
ArraySetIndex::ArraySetIndex(size_t ArrayPos,
							 std::shared_ptr<::arrow::Table> Table) : arrayPos_(ArrayPos), table_(std::move(Table))  {}

const std::shared_ptr<::arrow::Table> &ArraySetIndex::getTable() const {
  return table_;
}
