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

  /**
   * Generates the physical plan from the logical plan
   *
   * @param logicalPlan
   * @return
   */
  static std::shared_ptr<PhysicalPlan> generate(const LogicalPlan &logicalPlan);

};

}

#endif //NORMAL_NORMAL_PLAN_INCLUDE_NORMAL_PLAN_PLANNER_H
