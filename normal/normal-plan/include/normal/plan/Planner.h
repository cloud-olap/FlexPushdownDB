//
// Created by matt on 16/4/20.
//

#ifndef NORMAL_NORMAL_PLAN_INCLUDE_NORMAL_PLAN_PLANNER_H
#define NORMAL_NORMAL_PLAN_INCLUDE_NORMAL_PLAN_PLANNER_H

#include <memory>

#include "PhysicalPlan.h"
#include "LogicalPlan.h"

namespace normal::plan {

/**
 * Query planner, takes a logical plan and produces a physical one
 *
 * At the moment, this just means adding parallelism
 *
 */
class Planner {

public:
  static std::shared_ptr<PhysicalPlan> generate(std::shared_ptr<LogicalPlan> logicalPlan);

};

}

#endif //NORMAL_NORMAL_PLAN_INCLUDE_NORMAL_PLAN_PLANNER_H
