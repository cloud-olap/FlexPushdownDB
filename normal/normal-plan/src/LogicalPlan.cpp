//
// Created by matt on 16/4/20.
//

#include "normal/plan/LogicalPlan.h"

LogicalPlan::LogicalPlan(const std::shared_ptr<std::vector<std::shared_ptr<normal::plan::LogicalOperator>>> &Operators)
	: operators_(Operators) {}

const std::shared_ptr<std::vector<std::shared_ptr<normal::plan::LogicalOperator>>> &LogicalPlan::getOperators() const {
  return operators_;
}
