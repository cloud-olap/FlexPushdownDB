//
// Created by matt on 27/4/20.
//

#ifndef NORMAL_NORMAL_EXPRESSION_SIMPLE_INCLUDE_NORMAL_EXPRESSION_SIMPLE_EXPRESSION_H
#define NORMAL_NORMAL_EXPRESSION_SIMPLE_INCLUDE_NORMAL_EXPRESSION_SIMPLE_EXPRESSION_H

#include <string>
#include <arrow/type.h>
#include <tl/expected.hpp>

namespace normal::expression::simple {

class Expression {

public:
  virtual ~Expression() = default;

  virtual void compile(std::shared_ptr<arrow::Schema> schema) = 0;
  [[nodiscard]] virtual std::shared_ptr<arrow::DataType> resultType(std::shared_ptr<arrow::Schema>) = 0;

  [[nodiscard]] virtual std::string &alias() = 0;
  [[nodiscard]] const std::shared_ptr<arrow::DataType> &getReturnType() const;

  virtual tl::expected<std::shared_ptr<arrow::Array>, std::string> evaluate(const arrow::RecordBatch &recordBatch) = 0;

protected:

  /**
   * This is only know after the expression has been evaluated, e.g. a column expression can only know its return type once
   * its inspected the input schema
   */
  std::shared_ptr<arrow::DataType> returnType_;

};

}

#endif //NORMAL_NORMAL_EXPRESSION_SIMPLE_INCLUDE_NORMAL_EXPRESSION_SIMPLE_EXPRESSION_H
