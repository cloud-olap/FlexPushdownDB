//
// Created by matt on 27/4/20.
//

#ifndef NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_COLUMN_H
#define NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_COLUMN_H

#include <string>
#include <memory>

#include "Expression.h"

namespace normal::expression::gandiva {

class Column : public Expression {

public:
  explicit Column(std::string columnName);

  ::gandiva::NodePtr buildGandivaExpression(std::shared_ptr<arrow::Schema> Ptr) override;
  std::shared_ptr<arrow::DataType> resultType(std::shared_ptr<arrow::Schema> Ptr) override;
  void compile(std::shared_ptr<arrow::Schema> schema) override;
  std::string name() override;

private:
  std::string columnName_;

};

static std::shared_ptr<Expression> col(std::string columnName) {
  return std::make_shared<Column>(std::move(columnName));
}

}

#endif //NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_COLUMN_H
