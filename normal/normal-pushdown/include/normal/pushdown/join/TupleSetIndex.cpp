//
// Created by matt on 1/8/20.
//

#include "TupleSetIndex.h"

#include <utility>

using namespace normal::pushdown::join;

TupleSetIndex::TupleSetIndex(size_t columnIndex, std::shared_ptr<::arrow::Table> table) :
	columnIndex_(columnIndex),
	table_(std::move(table)) {}

const std::shared_ptr<::arrow::Table> &TupleSetIndex::getTable() const {
  return table_;
}

std::shared_ptr<::arrow::DataType> TupleSetIndex::type() {
  return table_->column(columnIndex_)->type();
}

int64_t TupleSetIndex::size() {
  return table_->num_rows();
}

std::vector<std::shared_ptr<::arrow::ChunkedArray>> TupleSetIndex::columns() {
  return table_->columns();
}
