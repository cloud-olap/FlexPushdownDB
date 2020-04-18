//
// Created by matt on 16/4/20.
//

#include <normal/plan/PhysicalPlan.h>

using namespace normal::plan;

PhysicalPlan::PhysicalPlan() :
	operators_(std::make_shared<std::unordered_map<std::string, std::shared_ptr<normal::core::Operator>>>()) {}

void PhysicalPlan::put(std::shared_ptr<normal::core::Operator> operator_) {
  operators_->emplace(operator_->name(), operator_);
}

const std::shared_ptr<std::unordered_map<std::string,
										 std::shared_ptr<normal::core::Operator>>> &PhysicalPlan::getOperators() const {
  return operators_;
}

