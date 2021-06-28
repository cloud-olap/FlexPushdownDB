//
// Created by matt on 27/4/20.
//

#include "normal/expression/gandiva/Column.h"

#include <gandiva/tree_expr_builder.h>

#include <normal/tuple/ColumnName.h>

using namespace normal::expression::gandiva;

Column::Column(std::string columnName): columnName_(std::move(columnName)) {
}

void Column::compile(std::shared_ptr<arrow::Schema> schema) {
  auto field = schema->GetFieldByName(columnName_);
  if(field == nullptr){
    // FIXME
    throw std::runtime_error("Column '" + columnName_ + "' does not exist");
  }
  gandivaExpression_ = ::gandiva::TreeExprBuilder::MakeField(field);
  returnType_ = field->type();
}

std::string Column::alias() {
  return columnName_;
}

const std::string &Column::getColumnName() const {
  return columnName_;
}

std::shared_ptr<std::vector<std::string> > Column::involvedColumnNames() {
  auto involvedColumnNames = std::make_shared<std::vector<std::string>>();
  involvedColumnNames->emplace_back(columnName_);
  return involvedColumnNames;
}

std::shared_ptr<Expression> normal::expression::gandiva::col(const std::string& columnName) {
  auto canonicalColumnName = normal::tuple::ColumnName::canonicalize(columnName);
  return std::make_shared<Column>(canonicalColumnName);
}
