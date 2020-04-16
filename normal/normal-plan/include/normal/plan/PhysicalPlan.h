//
// Created by matt on 16/4/20.
//

#ifndef NORMAL_NORMAL_PLAN_INCLUDE_NORMAL_PLAN_PHYSICALPLAN_H
#define NORMAL_NORMAL_PLAN_INCLUDE_NORMAL_PLAN_PHYSICALPLAN_H

#include <memory>
#include <unordered_map>

#include <normal/core/Operator.h>

class PhysicalPlan {

public:
  PhysicalPlan();
  void put(std::shared_ptr<normal::core::Operator> operator_);
  std::shared_ptr<normal::core::Operator> get(std::string operatorName);

private:
  std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<normal::core::Operator>>> operators_;
public:
  const std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<normal::core::Operator>>> &getOperators() const;

};

#endif //NORMAL_NORMAL_PLAN_INCLUDE_NORMAL_PLAN_PHYSICALPLAN_H
