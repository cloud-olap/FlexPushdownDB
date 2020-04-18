//
// Created by matt on 16/4/20.
//

#include "normal/plan/PhysicalPlan.h"

using namespace normal::plan;

PhysicalPlan::PhysicalPlan()
	: operators_(std::make_shared<std::unordered_map<std::string, std::shared_ptr<normal::core::Operator>>>()) {}

void PhysicalPlan::put(std::shared_ptr<normal::core::Operator> operator_) {
  operators_->emplace(operator_->name(), operator_);
}

tl::expected<std::shared_ptr<normal::core::Operator>, std::string> PhysicalPlan::get(std::string operatorName) {
  auto entry = operators_->find(operatorName);
  if(entry == operators_->end())
	return tl::unexpected("No entry for operator '" + operatorName + "'");
  else
	return entry->second;
}

const std::shared_ptr<std::unordered_map<std::string,
										 std::shared_ptr<normal::core::Operator>>> &PhysicalPlan::getOperators() const {
  return operators_;
}

