//
// Created by matt on 21/4/20.
//

#ifndef NORMAL_NORMAL_EXPRESSION_INCLUDE_NORMAL_EXPRESSION_PROJECTOR_H
#define NORMAL_NORMAL_EXPRESSION_INCLUDE_NORMAL_EXPRESSION_PROJECTOR_H

#include <vector>
#include <memory>
#include "Expression.h"

namespace normal::expression {

/**
 * A projector is a collection of expressions
 *
 * This class is necessary so expressions can be compiled and then reused over multiple invocations of an operator
 */
class Projector {

public:
  Projector(const std::vector<std::shared_ptr<Expression>> &Expressions);

  void compile(std::shared_ptr<arrow::Schema> schema);

private:
  std::vector<std::shared_ptr<Expression>> expressions_;

};

}

#endif //NORMAL_NORMAL_EXPRESSION_INCLUDE_NORMAL_EXPRESSION_PROJECTOR_H
