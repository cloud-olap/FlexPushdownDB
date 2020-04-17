//
// Created by matt on 1/4/20.
//

#include "normal/plan/CollateLogicalOperator.h"

#include <normal/pushdown/Collate.h>

#include <normal/plan/OperatorTypes.h>

normal::plan::CollateLogicalOperator::CollateLogicalOperator()
	: LogicalOperator(OperatorTypes::collateOperatorType()) {}

std::shared_ptr<normal::core::Operator> normal::plan::CollateLogicalOperator::toOperator() {
  return std::make_shared<normal::pushdown::Collate>("collate");
}
std::shared_ptr<std::vector<std::shared_ptr<normal::core::Operator>>> normal::plan::CollateLogicalOperator::toOperators() {
  auto operators = std::make_shared<std::vector<std::shared_ptr<core::Operator>>>();
  operators->push_back(this->toOperator());
  return operators;
}

