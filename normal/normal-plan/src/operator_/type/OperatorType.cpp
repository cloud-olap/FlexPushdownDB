//
// Created by matt on 16/4/20.
//

#include <normal/plan/operator_/type/OperatorType.h>

using namespace normal::plan::operator_::type;

OperatorType::OperatorType(OperatorTypeId Id) : id_(Id) {}

bool OperatorType::is(std::shared_ptr<OperatorType> type) {
  return id_ == type->id_;
}

std::string OperatorType::toString() {
  switch (id_) {
  case OperatorTypeId::Scan: return "scan";
  case OperatorTypeId::Project: return "project";
  case OperatorTypeId::Aggregate: return "aggregate";
  case OperatorTypeId::Collate: return "collate";
  }
}




