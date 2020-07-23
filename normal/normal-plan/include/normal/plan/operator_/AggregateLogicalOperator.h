//
// Created by matt on 2/4/20.
//

#ifndef NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_LOGICAL_AGGREGATELOGICALOPERATOR_H
#define NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_LOGICAL_AGGREGATELOGICALOPERATOR_H

#include <vector>
#include <memory>

#include <normal/core/Operator.h>

#include <normal/plan/function/AggregateLogicalFunction.h>
#include <normal/plan/operator_/LogicalOperator.h>

using namespace normal::expression::gandiva;
namespace normal::plan::operator_ {

class AggregateLogicalOperator : public LogicalOperator {

public:
  explicit AggregateLogicalOperator(std::shared_ptr<std::vector<std::shared_ptr<function::AggregateLogicalFunction>>> functions,
                                    std::shared_ptr<LogicalOperator> producer);

  std::shared_ptr<std::vector<std::shared_ptr<core::Operator>>> toOperators();

  const std::shared_ptr<LogicalOperator> &getProducer() const;


  void setNumConcurrentUnits(int numConcurrentUnits);

private:
  std::shared_ptr<std::vector<std::shared_ptr<function::AggregateLogicalFunction>>> functions_;

  std::shared_ptr<LogicalOperator> producer_;

  int numConcurrentUnits;

};

std::shared_ptr<normal::expression::gandiva::Expression> castToFloat64Type(std::shared_ptr<normal::expression::gandiva::Expression> expr);

}

#endif //NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_LOGICAL_AGGREGATELOGICALOPERATOR_H
