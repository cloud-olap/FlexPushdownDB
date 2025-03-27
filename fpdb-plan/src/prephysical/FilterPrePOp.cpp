//
// Created by Yifei Yang on 11/8/21.
//

#include <fpdb/plan/prephysical/FilterPrePOp.h>

namespace fpdb::plan::prephysical {

FilterPrePOp::FilterPrePOp(uint id,
                           double rowCount,
                           const shared_ptr<fpdb::expression::gandiva::Expression> &predicate) :
  PrePhysicalOp(id, FILTER, rowCount),
  predicate_(predicate) {}

string FilterPrePOp::getTypeString() {
  return "FilterPrePOp";
}

set<string> FilterPrePOp::getUsedColumnNames() {
  set<string> usedColumnNames = getProjectColumnNames();
  const auto &predicateColumnNames = predicate_->involvedColumnNames();
  usedColumnNames.insert(predicateColumnNames.begin(), predicateColumnNames.end());
  return usedColumnNames;
}

const shared_ptr<fpdb::expression::gandiva::Expression> &FilterPrePOp::getPredicate() const {
  return predicate_;
}

bool FilterPrePOp::equalTo(const std::shared_ptr<PrePhysicalOp> &other) const {
  // self
  if (type_ != other->getType()) {
    return false;
  }
  auto typedOther = std::static_pointer_cast<FilterPrePOp>(other);
  if (!expression::gandiva::Expression::equals(predicate_, typedOther->predicate_)) {
    return false;
  }
  // producers
  return equals(producers_, typedOther->producers_);
}

}
