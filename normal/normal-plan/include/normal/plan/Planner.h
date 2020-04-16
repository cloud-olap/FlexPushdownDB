//
// Created by matt on 16/4/20.
//

#ifndef NORMAL_NORMAL_PLAN_INCLUDE_NORMAL_PLAN_PLANNER_H
#define NORMAL_NORMAL_PLAN_INCLUDE_NORMAL_PLAN_PLANNER_H

#include <memory>
#include "PhysicalPlan.h"
#include "LogicalPlan.h"

/**
 *
 */
class Planner {

public:
  static std::shared_ptr<PhysicalPlan> generate(std::shared_ptr<LogicalPlan> logicalPlan);
};

#endif //NORMAL_NORMAL_PLAN_INCLUDE_NORMAL_PLAN_PLANNER_H
