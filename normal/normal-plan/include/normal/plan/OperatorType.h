//
// Created by matt on 16/4/20.
//

#ifndef NORMAL_NORMAL_PLAN_INCLUDE_NORMAL_PLAN_OPERATORTYPE_H
#define NORMAL_NORMAL_PLAN_INCLUDE_NORMAL_PLAN_OPERATORTYPE_H

#include <memory>

#include <normal/plan/OperatorTypeId.h>

class OperatorType {
public:

  explicit OperatorType(OperatorTypeId Id);
  ~OperatorType()  = default;

  bool is(std::shared_ptr<OperatorType> type);

private:
  OperatorTypeId id_;

};

#endif //NORMAL_NORMAL_PLAN_INCLUDE_NORMAL_PLAN_OPERATORTYPE_H
