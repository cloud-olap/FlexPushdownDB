//
// Created by matt on 21/4/20.
//

#include <normal/expression/Column.h>

using namespace normal::expression;

Column::Column(std::string columnName) : columnName_(std::move(columnName)) {
}

const std::string &Column::columnName() const {
  return columnName_;
}

void Column::compile(std::shared_ptr<arrow::Schema> schema) {
  gandivaExpression_ = buildGandivaExpression(schema);
  returnType_ = resultType(schema);
}

std::string &Column::name() {
  return columnName_;
}

gandiva::NodePtr Column::buildGandivaExpression(std::shared_ptr<arrow::Schema> schema) {
  return gandiva::TreeExprBuilder::MakeField(schema->GetFieldByName(columnName_));
}

std::shared_ptr<arrow::DataType> Column::resultType(std::shared_ptr<arrow::Schema> schema) {
  return schema->GetFieldByName(columnName_)->type();
}
