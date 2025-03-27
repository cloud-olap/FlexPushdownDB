//
// Created by Yifei Yang on 11/1/21.
//

#include <fpdb/plan/prephysical/AggregatePrePOp.h>

namespace fpdb::plan::prephysical {

AggregatePrePOp::AggregatePrePOp(uint id,
                                 double rowCount,
                                 const vector<string> &aggOutputColumnNames,
                                 const vector<shared_ptr<AggregatePrePFunction>> &functions):
  PrePhysicalOp(id, AGGREGATE, rowCount),
  aggOutputColumnNames_(aggOutputColumnNames),
  functions_(functions) {}

const vector<string> &AggregatePrePOp::getAggOutputColumnNames() const {
  return aggOutputColumnNames_;
}

const vector<shared_ptr<AggregatePrePFunction>> &AggregatePrePOp::getFunctions() const {
  return functions_;
}

string AggregatePrePOp::getTypeString() {
  return "AggregatePrePOp";
}

set<string> AggregatePrePOp::getUsedColumnNames() {
  set<string> usedColumnNames;
  for (const auto &function: functions_) {
    const auto involvedColumnNames = function->involvedColumnNames();
    usedColumnNames.insert(involvedColumnNames.begin(), involvedColumnNames.end());
  }
  return usedColumnNames;
}

bool AggregatePrePOp::equalTo(const std::shared_ptr<PrePhysicalOp> &other) const {
  // self
  if (type_ != other->getType()) {
    return false;
  }
  auto typedOther = std::static_pointer_cast<AggregatePrePOp>(other);
  if (!(aggOutputColumnNames_ == typedOther->aggOutputColumnNames_ &&
       AggregatePrePFunction::equals(functions_, typedOther->functions_))) {
    return false;
  }
  // producers
  return equals(producers_, typedOther->producers_);
}

}