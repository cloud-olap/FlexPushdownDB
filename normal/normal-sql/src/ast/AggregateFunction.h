//
// Created by matt on 2/4/20.
//

#ifndef NORMAL_NORMAL_SQL_SRC_AST_AGGREGATEFUNCTION_H
#define NORMAL_NORMAL_SQL_SRC_AST_AGGREGATEFUNCTION_H

#include <string>
#include <memory>
#include "AggregateExpression.h"
class AggregateFunction {
private:
  std::string type_;
public:
  explicit AggregateFunction(std::string type);
  std::shared_ptr<AggregateExpression> expression;
};

#endif //NORMAL_NORMAL_SQL_SRC_AST_AGGREGATEFUNCTION_H
