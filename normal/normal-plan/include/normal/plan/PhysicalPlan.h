//
// Created by matt on 16/4/20.
//

#ifndef NORMAL_NORMAL_PLAN_INCLUDE_NORMAL_PLAN_PHYSICALPLAN_H
#define NORMAL_NORMAL_PLAN_INCLUDE_NORMAL_PLAN_PHYSICALPLAN_H

#include <memory>
#include <unordered_map>

#include <tl/expected.hpp>

#include <normal/core/Operator.h>

namespace normal::plan {

/**
 * A physical query plan
 *
 * At the moment a collection of connected operators.
 */
class PhysicalPlan {

public:
  PhysicalPlan();

  void put(std::shared_ptr<core::Operator> operator_);

  [[nodiscard]] const std::shared_ptr<std::unordered_map<std::string,
														 std::shared_ptr<core::Operator>>>
  &getOperators() const;

private:
  std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<core::Operator>>> operators_;

};

}

#endif //NORMAL_NORMAL_PLAN_INCLUDE_NORMAL_PLAN_PHYSICALPLAN_H
