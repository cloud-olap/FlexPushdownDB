//
// Created by matt on 11/12/19.
//

#ifndef NORMAL_NORMAL_NORMAL_PUSHDOWN_SRC_AGGREGATEEXPRESSION_H
#define NORMAL_NORMAL_NORMAL_PUSHDOWN_SRC_AGGREGATEEXPRESSION_H

#include "normal/core/TupleSet.h"

class AggregateExpression {
private:
  std::shared_ptr<TupleSet> (*m_fn)(std::shared_ptr<TupleSet>, std::shared_ptr<TupleSet>);
public:
  explicit AggregateExpression(std::shared_ptr<TupleSet> (*fn)(std::shared_ptr<TupleSet>, std::shared_ptr<TupleSet> aggregateTupleSet));
  std::shared_ptr<TupleSet> apply(std::shared_ptr<TupleSet> tupleSet, std::shared_ptr<TupleSet> aggregateTupleSet);
};

#endif //NORMAL_NORMAL_NORMAL_PUSHDOWN_SRC_AGGREGATEEXPRESSION_H
