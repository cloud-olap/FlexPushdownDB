//
// Created by matt on 2/4/20.
//

#ifndef NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_EXPRESSION_COLUMN_H
#define NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_EXPRESSION_COLUMN_H


#include <memory>
#include <utility>
#include <arrow/api.h>
#include "gandiva/tree_expr_builder.h"

#include "Expression.h"

namespace normal::core::expression {

class Column : public Expression {
private:
  std::string columnName_;

public:
  explicit Column(std::string columnName) : columnName_(std::move(columnName)) {}

  [[nodiscard]] const std::string &columnName() const {
    return columnName_;
  }

  gandiva::NodePtr buildGandivaExpression(std::shared_ptr<arrow::Schema> schema) override {
    return gandiva::TreeExprBuilder::MakeField(schema->GetFieldByName(columnName_));
  }

  std::shared_ptr<arrow::DataType> resultType(std::shared_ptr<arrow::Schema> schema) override {
    return schema->GetFieldByName(columnName_)->type();
  }
};

static std::shared_ptr<Expression> col(std::string columnName){
  return std::make_shared<Column>(std::move(columnName));
}

}


#endif //NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_EXPRESSION_COLUMN_H
