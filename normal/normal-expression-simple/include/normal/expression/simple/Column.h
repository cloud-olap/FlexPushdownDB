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

  std::string &alias() override;
  std::shared_ptr<arrow::DataType> resultType(std::shared_ptr<arrow::Schema>) override;

  tl::expected<std::shared_ptr<arrow::Array>, std::string> evaluate(const arrow::RecordBatch &recordBatch) override;

private:
  std::string columnName_;

};

static std::shared_ptr<Expression> col(std::string columnName) {
  return std::make_shared<Column>(std::move(columnName));
}

}

#endif //NORMAL_NORMAL_EXPRESSION_SIMPLE_INCLUDE_NORMAL_EXPRESSION_SIMPLE_COLUMN_H
