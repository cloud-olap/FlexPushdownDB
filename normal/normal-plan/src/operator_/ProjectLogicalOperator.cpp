//
// Created by matt on 14/4/20.
//

#include <normal/plan/operator_/ProjectLogicalOperator.h>


#include <normal/pushdown/Project.h>
#include <normal/plan/operator_/type/OperatorTypes.h>

using namespace normal::plan::operator_;

ProjectLogicalOperator::ProjectLogicalOperator(
	std::shared_ptr<std::vector<std::shared_ptr<normal::expression::gandiva::Expression>>> expressions,
  std::shared_ptr<LogicalOperator> producer) :
	LogicalOperator(type::OperatorTypes::projectOperatorType()),
	expressions_(std::move(expressions)),
	producer_(std::move(producer)) {}

const std::shared_ptr<std::vector<std::shared_ptr<normal::expression::gandiva::Expression>>> &ProjectLogicalOperator::expressions() const {
  return expressions_;
}


std::shared_ptr<std::vector<std::shared_ptr<normal::core::Operator>>> ProjectLogicalOperator::toOperators() {
  auto operators = std::make_shared<std::vector<std::shared_ptr<core::Operator>>>();

  for (auto index = 0; index < numConcurrentUnits_; index++) {
    // FIXME: Defaulting to name -> proj
    auto project = std::make_shared<normal::pushdown::Project>(fmt::format("proj-{}", index), *expressions_);
    operators->emplace_back(project);
  }

  return operators;
}

const std::shared_ptr<LogicalOperator> &ProjectLogicalOperator::getProducer() const {
  return producer_;
}

void ProjectLogicalOperator::setNumConcurrentUnits(int numConcurrentUnits) {
  numConcurrentUnits_ = numConcurrentUnits;
}
