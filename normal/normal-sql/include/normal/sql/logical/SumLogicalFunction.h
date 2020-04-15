//
// Created by matt on 7/4/20.
//

#ifndef NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_LOGICAL_SUMLOGICALFUNCTION_H
#define NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_LOGICAL_SUMLOGICALFUNCTION_H

#include <memory>

#include <normal/pushdown/aggregate/AggregationFunction.h>

#include <normal/sql/logical/AggregateLogicalFunction.h>

namespace normal::sql::logical {

class SumLogicalFunction : public normal::sql::logical::AggregateLogicalFunction {
public:
  explicit SumLogicalFunction();
  std::shared_ptr<pushdown::aggregate::AggregationFunction> toExecutorFunction() override;

};

}

#endif //NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_LOGICAL_SUMLOGICALFUNCTION_H
