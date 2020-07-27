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
 * It has two functionalities:
 * 1) add parallelism
 * 2) add hybrid framework (pushdown + caching)
 *
 */
class Planner {

public:

  /**
   * Generate the full-pushdown physical plan from the logical plan
   *
   * @param logicalPlan
   * @return physicalPlan
   */
  static std::shared_ptr<PhysicalPlan> generateFullPushdown(const LogicalPlan &logicalPlan);

  /**
   * Generate the pullup-caching physical plan from the logical plan
   * Currently use LRU replacement policy, note LRU leads to pullup-caching, cause every segment to be accessed will always be least recently used
   *
   * @param logicalPlan
   * @return physicalPlan
   */
  static std::shared_ptr<PhysicalPlan> generatePullupCaching(const LogicalPlan &logicalPlan);

};

}

#endif //NORMAL_NORMAL_PLAN_INCLUDE_NORMAL_PLAN_PLANNER_H
