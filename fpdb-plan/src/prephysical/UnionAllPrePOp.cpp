//
// Created by Yifei Yang on 12/11/24.
//

#include <fpdb/plan/prephysical/UnionAllPrePOp.h>

namespace fpdb::plan::prephysical {

UnionAllPrePOp::UnionAllPrePOp(uint id, double rowCount):
  PrePhysicalOp(id, UNION_ALL, rowCount) {}

string UnionAllPrePOp::getTypeString() {
    return "UnionAllPrePOp";
}

set<string> UnionAllPrePOp::getUsedColumnNames() {
  return getProjectColumnNames();
}

bool UnionAllPrePOp::equalTo(const std::shared_ptr<PrePhysicalOp> &other) const {
  // self
  if (type_ != other->getType()) {
    return false;
  }
  // producers
  auto typedOther = std::static_pointer_cast<UnionAllPrePOp>(other);
  return equals(producers_, typedOther->producers_);
}

}
