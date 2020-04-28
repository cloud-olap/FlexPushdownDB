//
// Created by matt on 2/4/20.
//

#ifndef NORMAL_NORMAL_EXPRESSION_INCLUDE_NORMAL_EXPRESSION_EXPRESSION_H
#define NORMAL_NORMAL_EXPRESSION_INCLUDE_NORMAL_EXPRESSION_EXPRESSION_H

#include <utility>

#include <arrow/api.h>
#include <gandiva/node.h>

#include <normal/core/type/Type.h>

namespace normal::expression {

class Expression {

public:
  virtual ~Expression() = default;

  [[nodiscard]] const std::shared_ptr<arrow::DataType> &getReturnType() const;
  virtual void compile(std::shared_ptr<arrow::Schema> schema) = 0;

  [[nodiscard]] virtual std::string &name() = 0;

protected:

  /**
   * This is only know after the expression has been evaluated, e.g. a column expression can only know its return type once
   * its inspected the input schema
   */
  std::shared_ptr<arrow::DataType> returnType_;

};

}

#endif //NORMAL_NORMAL_EXPRESSION_INCLUDE_NORMAL_EXPRESSION_EXPRESSION_H
