//
// Created by Yifei Yang on 7/14/20.
//

#include "normal/plan/operator_/JoinLogicalOperator.h"
#include <normal/plan/operator_/type/OperatorTypes.h>
#include <normal/pushdown/join/HashJoinBuild.h>
#include <normal/pushdown/join/HashJoinProbe.h>
#include <normal/plan/Globals.h>

#include <utility>

using namespace normal::plan::operator_;

JoinLogicalOperator::JoinLogicalOperator(std::string leftColumnName, std::string rightColumnName,
                                         std::shared_ptr<LogicalOperator> leftProducer,
                                         std::shared_ptr<LogicalOperator> rightProducer)
        : LogicalOperator(type::OperatorTypes::joinOperatorType()),
          leftColumnName_(std::move(leftColumnName)), rightColumnName_(std::move(rightColumnName)),
          leftProducer_(std::move(leftProducer)), rightProducer_(std::move(rightProducer)){}

std::shared_ptr<std::vector<std::shared_ptr<normal::core::Operator>>> JoinLogicalOperator::toOperators() {
  const int numConcurrentUnits = JoinParallelDegree;
  auto operators = std::make_shared<std::vector<std::shared_ptr<normal::core::Operator>>>();

  // join build
  for (auto index = 0; index < numConcurrentUnits; index++) {
    auto joinBuild = std::make_shared<normal::pushdown::join::HashJoinBuild>(
            fmt::format("join-build-{}-{}-{}", leftColumnName_, rightColumnName_, index),
            leftColumnName_,
            getQueryId());
    operators->emplace_back(joinBuild);
  }

  // join probe
  for (auto index = 0; index < numConcurrentUnits; index++) {
    auto joinProbe = std::make_shared<normal::pushdown::join::HashJoinProbe>(
            fmt::format("join-probe-{}-{}-{}", leftColumnName_, rightColumnName_, index),
            normal::pushdown::join::JoinPredicate::create(leftColumnName_,rightColumnName_),
            neededColumnNames_,
            getQueryId());
    operators->emplace_back(joinProbe);
  }

  // wire up internally
  size_t size = operators->size();
  for (auto index = 0; index < size / 2; index++) {
    operators->at(index)->produce(operators->at(index + size / 2));
    operators->at(index + size / 2)->consume(operators->at(index));
  }

  return operators;
}

const std::shared_ptr<LogicalOperator> &JoinLogicalOperator::getLeftProducer() const {
  return leftProducer_;
}

[[maybe_unused]] const std::shared_ptr<LogicalOperator> &JoinLogicalOperator::getRightProducer() const {
  return rightProducer_;
}

const std::string &JoinLogicalOperator::getLeftColumnName() const {
  return leftColumnName_;
}

const std::string &JoinLogicalOperator::getRightColumnName() const {
  return rightColumnName_;
}

void JoinLogicalOperator::setNeededColumnNames(const std::set<std::string> &neededColumnNames) {
  neededColumnNames_ = neededColumnNames;
}

const std::set<std::string> &JoinLogicalOperator::getNeededColumnNames() const {
  return neededColumnNames_;
}

