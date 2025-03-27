//
// Created by Yifei Yang on 11/7/21.
//

#include <fpdb/plan/prephysical/HashJoinPrePOp.h>

namespace fpdb::plan::prephysical {

HashJoinPrePOp::HashJoinPrePOp(uint id,
                               double rowCount,
                               JoinType joinType,
                               const vector<string> &leftColumnNames,
                               const vector<string> &rightColumnNames,
                               bool pushable) :
  PrePhysicalOp(id, HASH_JOIN, rowCount),
  joinType_(joinType),
  leftColumnNames_(leftColumnNames),
  rightColumnNames_(rightColumnNames),
  pushable_(pushable) {}

string HashJoinPrePOp::getTypeString() {
  return "HashJoinPrePOp";
}

set<string> HashJoinPrePOp::getUsedColumnNames() {
  set<string> usedColumnNames = getProjectColumnNames();
  usedColumnNames.insert(leftColumnNames_.begin(), leftColumnNames_.end());
  usedColumnNames.insert(rightColumnNames_.begin(), rightColumnNames_.end());
  return usedColumnNames;
}

void HashJoinPrePOp::switchSide() {
  auto expReversedJoinType = reverseJoinType(joinType_);
  if (!expReversedJoinType.has_value()) {
    throw std::runtime_error(expReversedJoinType.error());
  }
  joinType_ = *expReversedJoinType;
  std::swap(leftColumnNames_, rightColumnNames_);
  std::swap(producers_[0], producers_[1]);
}

JoinType HashJoinPrePOp::getJoinType() const {
  return joinType_;
}

const vector<string> &HashJoinPrePOp::getLeftColumnNames() const {
  return leftColumnNames_;
}

const vector<string> &HashJoinPrePOp::getRightColumnNames() const {
  return rightColumnNames_;
}

bool HashJoinPrePOp::isPushable() const {
  return pushable_;
}

int HashJoinPrePOp::getNumJoinColumnPairs() const {
  return leftColumnNames_.size();
}

bool HashJoinPrePOp::equalTo(const std::shared_ptr<PrePhysicalOp> &other) const {
  // self
  if (type_ != other->getType()) {
    return false;
  }
  auto typedOther = std::static_pointer_cast<HashJoinPrePOp>(other);
  if (!(joinType_ == typedOther->joinType_ && leftColumnNames_ == typedOther->leftColumnNames_ &&
       rightColumnNames_ == typedOther->rightColumnNames_)) {
    return false;
  }
  // producers
  return equals(producers_, typedOther->producers_);
}

}