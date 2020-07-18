//
// Created by matt on 16/4/20.
//

#include <normal/plan/operator_/type/OperatorTypes.h>

using namespace normal::plan::operator_::type;

std::shared_ptr<ScanOperatorType> OperatorTypes::scanOperatorType() {
  return std::make_shared<ScanOperatorType>();
}

std::shared_ptr<ProjectOperatorType> OperatorTypes::projectOperatorType() {
  return std::make_shared<ProjectOperatorType>();
}

std::shared_ptr<AggregateOperatorType> OperatorTypes::aggregateOperatorType() {
  return std::make_shared<AggregateOperatorType>();
}

std::shared_ptr<CollateOperatorType> OperatorTypes::collateOperatorType() {
  return std::make_shared<CollateOperatorType>();
}

std::shared_ptr<JoinOperatorType> OperatorTypes::joinOperatorType() {
  return std::make_shared<JoinOperatorType>();
}

std::shared_ptr<GroupOperatorType> OperatorTypes::groupOperatorType() {
  return std::shared_ptr<GroupOperatorType>();
}
