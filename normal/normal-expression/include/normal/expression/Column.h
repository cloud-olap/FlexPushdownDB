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

namespace normal::expression {

class Column : public Expression {

public:
  explicit Column(std::string columnName);

  [[nodiscard]] const std::string &columnName() const;

  [[nodiscard]] std::string &name() override;
  void compile(std::shared_ptr<arrow::Schema> schema) override;
  gandiva::NodePtr buildGandivaExpression(std::shared_ptr<arrow::Schema> schema) override;

  std::shared_ptr<arrow::DataType> resultType(std::shared_ptr<arrow::Schema> schema) override;

private:
  std::string columnName_;

};

static std::shared_ptr<Expression> col(std::string columnName) {
  return std::make_shared<Column>(std::move(columnName));
}

}

#endif //NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_EXPRESSION_COLUMN_H
