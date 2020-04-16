//
// Created by matt on 16/4/20.
//

#ifndef NORMAL_NORMAL_PLAN_INCLUDE_NORMAL_PLAN_PHYSICALPLAN_H
#define NORMAL_NORMAL_PLAN_INCLUDE_NORMAL_PLAN_PHYSICALPLAN_H

#include <memory>
#include <unordered_map>

#include <tl/expected.hpp>

#include <normal/core/Operator.h>

class PhysicalPlan {

public:
  PhysicalPlan();

  void put(std::shared_ptr<normal::core::Operator> operator_);
  tl::expected<std::shared_ptr<normal::core::Operator>, std::string> get(std::string operatorName);

  [[nodiscard]] const std::shared_ptr<std::unordered_map<std::string,
														 std::shared_ptr<normal::core::Operator>>>
  &getOperators() const;

private:
  std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<normal::core::Operator>>> operators_;

};

#endif //NORMAL_NORMAL_PLAN_INCLUDE_NORMAL_PLAN_PHYSICALPLAN_H
