//
// Created by matt on 16/4/20.
//

#ifndef NORMAL_NORMAL_PLAN_INCLUDE_NORMAL_PLAN_OPERATORTYPES_H
#define NORMAL_NORMAL_PLAN_INCLUDE_NORMAL_PLAN_OPERATORTYPES_H

#include <memory>

#include "ScanOperatorType.h"
#include "ProjectOperatorType.h"
#include "AggregateOperatorType.h"
#include "CollateOperatorType.h"
#include "JoinOperatorType.h"
#include "GroupOperatorType.h"

namespace normal::plan::operator_::type {

/**
 * Operator type factories
 */
class OperatorTypes {

public:
  static std::shared_ptr<ScanOperatorType> scanOperatorType();
  static std::shared_ptr<ProjectOperatorType> projectOperatorType();
  static std::shared_ptr<AggregateOperatorType> aggregateOperatorType();
  static std::shared_ptr<CollateOperatorType> collateOperatorType();
  static std::shared_ptr<JoinOperatorType> joinOperatorType();
  static std::shared_ptr<GroupOperatorType> groupOperatorType();

};

}

#endif //NORMAL_NORMAL_PLAN_INCLUDE_NORMAL_PLAN_OPERATORTYPES_H
