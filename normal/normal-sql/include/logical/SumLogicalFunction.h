//
// Created by matt on 7/4/20.
//

#ifndef NORMAL_NORMAL_SQL_SRC_AST_SUMLOGICALFUNCTION_H
#define NORMAL_NORMAL_SQL_SRC_AST_SUMLOGICALFUNCTION_H

#include "logical/AggregateLogicalFunction.h"

class SumLogicalFunction : public AggregateLogicalFunction {
public:
  explicit SumLogicalFunction();
  std::shared_ptr<normal::pushdown::aggregate::AggregationFunction> toExecutorFunction() override;

};

#endif //NORMAL_NORMAL_SQL_SRC_AST_SUMLOGICALFUNCTION_H
