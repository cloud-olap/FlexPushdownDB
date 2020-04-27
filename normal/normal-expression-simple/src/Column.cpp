//
// Created by matt on 27/4/20.
//

#include "normal/expression/simple/Column.h"

using namespace normal::expression::simple;

Column::Column(std::string columnName) : columnName_(std::move(columnName)) {
}

void Column::compile(std::shared_ptr<arrow::Schema> schema) {
  returnType_ = resultType(schema);
}

std::string &Column::alias() {
  return columnName_;
}

std::shared_ptr<arrow::DataType> Column::resultType(std::shared_ptr<arrow::Schema> schema) {
  return schema->GetFieldByName(columnName_)->type();
}

tl::expected<std::shared_ptr<arrow::Array>, std::string> Column::evaluate(const arrow::RecordBatch &recordBatch) {
  auto array = recordBatch.GetColumnByName(columnName_);
  return array;
}
