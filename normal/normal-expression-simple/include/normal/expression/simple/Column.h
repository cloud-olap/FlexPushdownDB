//
// Created by matt on 27/4/20.
//

#ifndef NORMAL_NORMAL_EXPRESSION_SIMPLE_INCLUDE_NORMAL_EXPRESSION_SIMPLE_COLUMN_H
#define NORMAL_NORMAL_EXPRESSION_SIMPLE_INCLUDE_NORMAL_EXPRESSION_SIMPLE_COLUMN_H

#include <string>
#include <memory>

#include <arrow/api.h>

#include "Expression.h"

namespace normal::expression::simple {

class Column : public Expression {

public:
  explicit Column(std::string columnName);

  void compile(std::shared_ptr<arrow::Schema> schema) override;
  std::string alias() override;

  tl::expected<std::shared_ptr<arrow::Array>, std::string> evaluate(const arrow::RecordBatch &batch) override;

private:
  std::string columnName_;

};

std::shared_ptr<Expression> col(std::string columnName);

}

#endif //NORMAL_NORMAL_EXPRESSION_SIMPLE_INCLUDE_NORMAL_EXPRESSION_SIMPLE_COLUMN_H
