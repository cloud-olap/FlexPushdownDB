//
// Created by matt on 27/4/20.
//

#include "fpdb/expression/gandiva/Column.h"
#include <fmt/format.h>
#include <gandiva/tree_expr_builder.h>

#include <fpdb/tuple/ColumnName.h>

using namespace fpdb::expression::gandiva;

Column::Column(std::string columnName):
  Expression(COLUMN),
  columnName_(std::move(columnName)) {}

void Column::compile(const std::shared_ptr<arrow::Schema> &schema) {
  auto field = schema->GetFieldByName(columnName_);
  if(field == nullptr){
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

std::set<std::string> Column::involvedColumnNames() {
  std::set<std::string> involvedColumnNames;
  involvedColumnNames.insert(columnName_);
  return involvedColumnNames;
}

std::string Column::getTypeString() const {
  return "Column";
}

::nlohmann::json Column::toJson() const {
  ::nlohmann::json jObj;
  jObj.emplace("type", getTypeString());
  jObj.emplace("columnName", columnName_);
  return jObj;
}

tl::expected<std::shared_ptr<Column>, std::string> Column::fromJson(const nlohmann::json &jObj) {
  if (!jObj.contains("columnName")) {
    return tl::make_unexpected(fmt::format("ColumnName not specified in Column expression JSON '{}'", to_string(jObj)));
  }
  auto columnName = jObj["columnName"].get<std::string>();
  return std::make_shared<Column>(columnName);
}

std::shared_ptr<Expression> fpdb::expression::gandiva::col(const std::string& columnName) {
  auto canonicalColumnName = fpdb::tuple::ColumnName::canonicalize(columnName);
  return std::make_shared<Column>(canonicalColumnName);
}
