//
// Created by matt on 2/4/20.
//

#ifndef NORMAL_NORMAL_SQL_SRC_AST_AGGREGATEFUNCTION_H
#define NORMAL_NORMAL_SQL_SRC_AST_AGGREGATEFUNCTION_H

#include <string>
#include <memory>
#include <normal/core/expression/Expression.h>
#include <normal/pushdown/aggregate/AggregationFunction.h>
#include "AggregateExpression.h"

class AggregateFunction {
private:
  std::string type_;
  std::shared_ptr<normal::core::expression::Expression> expression_;

public:
  explicit AggregateFunction(std::string type);
  virtual ~AggregateFunction() = default;

  std::shared_ptr<normal::core::expression::Expression> expression();
  void expression(const std::shared_ptr<normal::core::expression::Expression> &expression);

  virtual std::shared_ptr<normal::pushdown::aggregate::AggregationFunction> toExecutorFunction() = 0;

};

#endif //NORMAL_NORMAL_SQL_SRC_AST_AGGREGATEFUNCTION_H
