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

  /**
   * Invoked once before evaluation so expressions can run any initializations that can only be done at runtime (e.g.
   * where an expression needs an input schema) and can be reused across evaluations.
   *
   * @param schema
   */
  virtual void compile(std::shared_ptr<arrow::Schema> schema) = 0;

  /**
   * The alias of the evaluated expression (e.g. "select COL from TBL" should assign the alias "COL" to the
   * column expression COL)
   * @return
   */
  [[nodiscard]] virtual std::string alias() = 0;

protected:

  /**
   * This is only know after the expression has been evaluated, e.g. a column expression can only know its return type once
   * its inspected the input schema
   */
  std::shared_ptr<arrow::DataType> returnType_;

};

}

#endif //NORMAL_NORMAL_EXPRESSION_INCLUDE_NORMAL_EXPRESSION_EXPRESSION_H
