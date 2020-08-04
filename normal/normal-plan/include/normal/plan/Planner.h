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
   * Generate the physical plan from the logical plan
   *
   * @param logicalPlan
   * @param mode: full pushdown, pullup caching, hybrid caching
   * @return physicalPlan
   */
  static std::shared_ptr<PhysicalPlan> generate(const LogicalPlan &logicalPlan,
                                                        std::shared_ptr<normal::plan::operator_::mode::Mode> mode);

};

}

#endif //NORMAL_NORMAL_PLAN_INCLUDE_NORMAL_PLAN_PLANNER_H
