//
// Created by Yifei Yang on 7/14/20.
//

#include "normal/plan/operator_/JoinLogicalOperator.h"
#include <normal/plan/operator_/type/OperatorTypes.h>
#include <normal/pushdown/join/HashJoinBuild.h>
#include <normal/pushdown/join/HashJoinProbe.h>
#include <normal/plan/Globals.h>

using namespace normal::plan::operator_;

JoinLogicalOperator::JoinLogicalOperator(const std::string &leftColumnName, const std::string &rightColumnName,
                                         const std::shared_ptr<LogicalOperator> &leftProducer,
                                         const std::shared_ptr<LogicalOperator> &rightProducer)
        : LogicalOperator(type::OperatorTypes::joinOperatorType()),
          leftColumnName_(leftColumnName), rightColumnName_(rightColumnName),
          leftProducer_(leftProducer), rightProducer_(rightProducer){}

std::shared_ptr<std::vector<std::shared_ptr<normal::core::Operator>>> JoinLogicalOperator::toOperators() {
  const int numConcurrentUnits = JoinParallelDegree;
  auto operators = std::make_shared<std::vector<std::shared_ptr<normal::core::Operator>>>();

  // join build
  for (auto index = 0; index < numConcurrentUnits; index++) {
    auto joinBuild = std::make_shared<normal::pushdown::join::HashJoinBuild>(
            fmt::format("join-build-{}-{}-{}", leftColumnName_, rightColumnName_, index),
            leftColumnName_);
    operators->emplace_back(joinBuild);
  }

  // join probe
  for (auto index = 0; index < numConcurrentUnits; index++) {
    auto joinProbe = std::make_shared<normal::pushdown::join::HashJoinProbe>(
            fmt::format("join-probe-{}-{}-{}", leftColumnName_, rightColumnName_, index),
            normal::pushdown::join::JoinPredicate::create(leftColumnName_,rightColumnName_));
    operators->emplace_back(joinProbe);
  }

  // wire up internally
  int size = operators->size();
  for (auto index = 0; index < size / 2; index++) {
    operators->at(index)->produce(operators->at(index + size / 2));
    operators->at(index + size / 2)->consume(operators->at(index));
  }

  return operators;
}

const std::shared_ptr<LogicalOperator> &JoinLogicalOperator::getLeftProducer() const {
  return leftProducer_;
}

const std::shared_ptr<LogicalOperator> &JoinLogicalOperator::getRightProducer() const {
  return rightProducer_;
}

const std::string &JoinLogicalOperator::getLeftColumnName() const {
  return leftColumnName_;
}

const std::string &JoinLogicalOperator::getRightColumnName() const {
  return rightColumnName_;
}

