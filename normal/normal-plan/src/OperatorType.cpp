//
// Created by matt on 16/4/20.
//

#include <normal/plan/OperatorType.h>

OperatorType::OperatorType(OperatorTypeId Id) : id_(Id) {}

bool OperatorType::is(std::shared_ptr<OperatorType> type) {
  return id_ == type->id_;
}




