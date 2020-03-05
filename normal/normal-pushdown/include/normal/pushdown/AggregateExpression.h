//
// Created by matt on 11/12/19.
//

#ifndef NORMAL_NORMAL_NORMAL_PUSHDOWN_SRC_AGGREGATEEXPRESSION_H
#define NORMAL_NORMAL_NORMAL_PUSHDOWN_SRC_AGGREGATEEXPRESSION_H

#include "normal/core/TupleSet.h"

namespace normal::pushdown {

class AggregateExpression {

private:
  std::shared_ptr<normal::core::TupleSet> (*m_fn)(std::shared_ptr<normal::core::TupleSet>,
                                                  std::shared_ptr<normal::core::TupleSet>);

public:
  explicit AggregateExpression(std::shared_ptr<normal::core::TupleSet>  (*fn)(std::shared_ptr<normal::core::TupleSet>,
                                                                              std::shared_ptr<normal::core::TupleSet> aggregateTupleSet));
  std::shared_ptr<normal::core::TupleSet> apply(std::shared_ptr<normal::core::TupleSet> tupleSet,
                                                std::shared_ptr<normal::core::TupleSet> aggregateTupleSet);

};

}

#endif //NORMAL_NORMAL_NORMAL_PUSHDOWN_SRC_AGGREGATEEXPRESSION_H
