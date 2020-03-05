//
// Created by matt on 11/12/19.
//

#include "normal/pushdown/AggregateExpression.h"

#include <utility>

AggregateExpression::AggregateExpression(std::shared_ptr<normal::core::TupleSet> (*fn)(std::shared_ptr<normal::core::TupleSet>, std::shared_ptr<normal::core::TupleSet>)) {
  m_fn = fn;
}

std::shared_ptr<normal::core::TupleSet> AggregateExpression::apply(std::shared_ptr<normal::core::TupleSet> tupleSet, std::shared_ptr<normal::core::TupleSet> aggregatedTupleSet) {
  return m_fn(std::move(tupleSet), std::move(aggregatedTupleSet));
}

