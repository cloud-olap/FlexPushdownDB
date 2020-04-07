//
// Created by matt on 7/4/20.
//

#ifndef NORMAL_NORMAL_SQL_SRC_AST_SUMASTFUNCTION_H
#define NORMAL_NORMAL_SQL_SRC_AST_SUMASTFUNCTION_H

#include "AggregateFunction.h"

class SumASTFunction : public AggregateFunction {
public:
  explicit SumASTFunction();
  std::shared_ptr<normal::pushdown::aggregate::AggregationFunction> toExecutorFunction() override;
};

#endif //NORMAL_NORMAL_SQL_SRC_AST_SUMASTFUNCTION_H
