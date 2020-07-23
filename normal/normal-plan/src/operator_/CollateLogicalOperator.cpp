//
// Created by matt on 1/4/20.
//

#include <normal/plan/operator_/CollateLogicalOperator.h>


#include <normal/pushdown/Collate.h>

#include <normal/plan/operator_/type/OperatorTypes.h>

using namespace normal::plan::operator_;

CollateLogicalOperator::CollateLogicalOperator()
	: LogicalOperator(type::OperatorTypes::collateOperatorType()) {}


std::shared_ptr<std::vector<std::shared_ptr<normal::core::Operator>>> CollateLogicalOperator::toOperators() {
  auto collate = std::make_shared<normal::pushdown::Collate>("collate", 0);
  auto operators = std::make_shared<std::vector<std::shared_ptr<core::Operator>>>();
  operators->push_back(collate);
  return operators;
}

