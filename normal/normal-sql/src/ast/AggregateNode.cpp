//
// Created by matt on 2/4/20.
//

#include "AggregateNode.h"
#include <normal/pushdown/Aggregate.h>
#include <normal/pushdown/aggregate/Sum.h>

std::shared_ptr<normal::core::Operator> AggregateNode::toOperator() {

  auto sum = std::make_shared<normal::pushdown::aggregate::Sum>("sum(A)", "A");
  auto
      expressions02 =
      std::make_shared<std::vector<std::shared_ptr<normal::pushdown::aggregate::AggregationFunction>>>();
  expressions02->emplace_back(sum);
  auto aggregate02 = std::make_shared<normal::pushdown::Aggregate>("sum(A)", expressions02);

  return aggregate02;
}
