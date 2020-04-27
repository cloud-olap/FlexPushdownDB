//
// Created by matt on 27/4/20.
//

#ifndef NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_EXPRESSION_H
#define NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_EXPRESSION_H

#include <memory>
#include <arrow/type.h>
#include <gandiva/node.h>

namespace normal::expression::gandiva {

class Expression {

public:
  virtual ~Expression() = default;

  [[nodiscard]] const std::shared_ptr<arrow::DataType> &getReturnType() const;
  [[nodiscard]] const ::gandiva::NodePtr &getGandivaExpression() const;

  virtual ::gandiva::NodePtr buildGandivaExpression(std::shared_ptr<arrow::Schema>) = 0;
  virtual std::shared_ptr<arrow::DataType> resultType(std::shared_ptr<arrow::Schema>) = 0;
  virtual void compile(std::shared_ptr<arrow::Schema> schema) = 0;

  [[nodiscard]] virtual std::string &name() = 0;

  std::string showString();

protected:

  /**
   * This is only know after the expression has been evaluated, e.g. a column expression can only know its return type once
   * its inspected the input schema
   */
  std::shared_ptr<arrow::DataType> returnType_;
  ::gandiva::NodePtr gandivaExpression_;

};

}

#endif //NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_EXPRESSION_H
