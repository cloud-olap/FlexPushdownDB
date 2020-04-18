//
// Created by matt on 16/4/20.
//

#ifndef NORMAL_NORMAL_PLAN_INCLUDE_NORMAL_PLAN_LOGICALPLAN_H
#define NORMAL_NORMAL_PLAN_INCLUDE_NORMAL_PLAN_LOGICALPLAN_H

#include <memory>
#include <vector>
#include <experimental/filesystem>

#include <normal/plan/operator_/LogicalOperator.h>

using namespace std::experimental;

namespace normal::plan {

/**
 * A logical query plan. A collection of operators and their connections.
 */
class LogicalPlan {

public:
  explicit LogicalPlan(std::shared_ptr<std::vector<std::shared_ptr<operator_::LogicalOperator>>> operators);

  [[nodiscard]] const std::shared_ptr<std::vector<std::shared_ptr<operator_::LogicalOperator>>> &getOperators() const;

  /**
   * Generate an graph of the plan in SVG format
   *
   * @param path Path to write the SVG to
   */
  void writeGraph(const filesystem::path& path);

private:
  std::shared_ptr<std::vector<std::shared_ptr<operator_::LogicalOperator>>> operators_;

};

}

#endif //NORMAL_NORMAL_PLAN_INCLUDE_NORMAL_PLAN_LOGICALPLAN_H
