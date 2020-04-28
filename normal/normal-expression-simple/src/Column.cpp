//
// Created by matt on 27/4/20.
//

#include "normal/expression/simple/Column.h"

using namespace normal::expression::simple;

Column::Column(std::string columnName) : columnName_(std::move(columnName)) {
}

void Column::compile(std::shared_ptr<arrow::Schema> schema) {
  returnType_ = schema->GetFieldByName(columnName_)->type();
}

std::string Column::alias() {
  return columnName_;
}

tl::expected<std::shared_ptr<arrow::Array>, std::string> Column::evaluate(const arrow::RecordBatch &batch) {
  auto array = batch.GetColumnByName(columnName_);
  return array;
}

std::shared_ptr<Expression> normal::expression::simple::col(std::string columnName) {
  return std::make_shared<Column>(std::move(columnName));
}
