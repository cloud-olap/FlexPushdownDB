//
// Created by matt on 27/4/20.
//

#include "normal/expression/gandiva/Column.h"

#include <utility>
#include <gandiva/tree_expr_builder.h>

using namespace normal::expression::gandiva;

Column::Column(std::string columnName): columnName_(std::move(columnName)) {
}

::gandiva::NodePtr Column::buildGandivaExpression(std::shared_ptr<arrow::Schema> schema) {
  return ::gandiva::TreeExprBuilder::MakeField(schema->GetFieldByName(columnName_));
}

std::shared_ptr<arrow::DataType> Column::resultType(std::shared_ptr<arrow::Schema> schema) {
  return schema->GetFieldByName(columnName_)->type();
}

void Column::compile(std::shared_ptr<arrow::Schema> schema) {
  gandivaExpression_ = buildGandivaExpression(schema);
  returnType_ = resultType(schema);
}

std::string Column::name() {
  return columnName_;
}
