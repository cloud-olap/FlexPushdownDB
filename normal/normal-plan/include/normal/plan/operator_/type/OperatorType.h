//
// Created by matt on 16/4/20.
//

#ifndef NORMAL_NORMAL_PLAN_INCLUDE_NORMAL_PLAN_OPERATORTYPE_H
#define NORMAL_NORMAL_PLAN_INCLUDE_NORMAL_PLAN_OPERATORTYPE_H

#include <memory>

#include <normal/plan/operator_/type/OperatorTypeId.h>

namespace normal::plan::operator_::type {

class OperatorType {
public:

  explicit OperatorType(OperatorTypeId Id);
  ~OperatorType()  = default;

  bool is(const std::shared_ptr<OperatorType>& type);

  std::string toString();

private:
  OperatorTypeId id_;

};

}

#endif //NORMAL_NORMAL_PLAN_INCLUDE_NORMAL_PLAN_OPERATORTYPE_H
