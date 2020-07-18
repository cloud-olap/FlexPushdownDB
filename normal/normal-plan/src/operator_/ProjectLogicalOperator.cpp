//
// Created by matt on 14/4/20.
//

#include <normal/plan/operator_/ProjectLogicalOperator.h>


#include <normal/pushdown/Project.h>
#include <normal/plan/operator_/type/OperatorTypes.h>

using namespace normal::plan::operator_;

ProjectLogicalOperator::ProjectLogicalOperator(
	std::shared_ptr<std::vector<std::shared_ptr<normal::expression::gandiva::Expression>>> expressions) :
	LogicalOperator(type::OperatorTypes::projectOperatorType()),
	expressions_(std::move(expressions)) {}

const std::shared_ptr<std::vector<std::shared_ptr<normal::expression::gandiva::Expression>>> &ProjectLogicalOperator::expressions() const {
  return expressions_;
}

std::shared_ptr<normal::core::Operator> ProjectLogicalOperator::toOperator() {

  // FIXME: Defaulting to name -> proj
  auto projectPhysicalOperator = std::make_shared<normal::pushdown::Project>("proj", *expressions_);

  return projectPhysicalOperator;
}

std::shared_ptr<std::vector<std::shared_ptr<normal::core::Operator>>> ProjectLogicalOperator::toOperators() {
  auto operators = std::make_shared<std::vector<std::shared_ptr<core::Operator>>>();
  operators->push_back(this->toOperator());
  return operators;
}
