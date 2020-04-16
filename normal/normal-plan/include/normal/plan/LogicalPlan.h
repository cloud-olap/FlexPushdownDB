//
// Created by matt on 16/4/20.
//

#ifndef NORMAL_NORMAL_PLAN_INCLUDE_NORMAL_PLAN_LOGICALPLAN_H
#define NORMAL_NORMAL_PLAN_INCLUDE_NORMAL_PLAN_LOGICALPLAN_H

#include <memory>
#include <vector>

#include <normal/plan/LogicalOperator.h>

/**
 * A logical query plan. A collection of operators and their connections.
 */
class LogicalPlan {

public:
  LogicalPlan(const std::shared_ptr<std::vector<std::shared_ptr<normal::plan::LogicalOperator>>> &Operators);
  const std::shared_ptr<std::vector<std::shared_ptr<normal::plan::LogicalOperator>>> &getOperators() const;

private:
  std::shared_ptr<std::vector<std::shared_ptr<normal::plan::LogicalOperator>>> operators_;

};

#endif //NORMAL_NORMAL_PLAN_INCLUDE_NORMAL_PLAN_LOGICALPLAN_H
