//
// Created by matt on 16/4/20.
//

#include <normal/plan/operator_/type/OperatorType.h>
#include <string>

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
  case OperatorTypeId::Join: return "join";
  case OperatorTypeId::Group: return "group";
  default:
    /*
     * Shouldn't occur, but we'll throw a serious-ish exception if it ever does
     */
    throw std::domain_error("Cannot get string for operator type '" + std::to_string(id_ )+ "'. Unrecognized operator type");
  }
}




