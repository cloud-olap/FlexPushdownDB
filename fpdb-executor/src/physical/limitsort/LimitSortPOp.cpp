//
// Created by Yifei Yang on 12/6/21.
//

#include <fpdb/executor/physical/limitsort/LimitSortPOp.h>

namespace fpdb::executor::physical::limitsort {

LimitSortPOp::LimitSortPOp(const string &name,
                           const vector<string> &projectColumnNames,
                           int nodeId,
                           int64_t k,
                           const vector<SortKey> &sortKeys):
  PhysicalOp(name, LIMIT_SORT, projectColumnNames, nodeId),
  k_(k),
  sortKeys_(sortKeys) {}

std::string LimitSortPOp::getTypeString() const {
  return "LimitSortPOp";
}

void LimitSortPOp::onReceive(const Envelope &message) {
  if (message.message().type() == MessageType::START) {
    this->onStart();
  } else if (message.message().type() == MessageType::TUPLESET) {
    auto tupleSetMessage = dynamic_cast<const TupleSetMessage &>(message.message());
    this->onTupleSet(tupleSetMessage);
  } else if (message.message().type() == MessageType::COMPLETE) {
    auto completeMessage = dynamic_cast<const CompleteMessage &>(message.message());
    this->onComplete(completeMessage);
  } else {
    ctx()->notifyError("Unrecognized message type " + message.message().getTypeString());
  }
}

void LimitSortPOp::onStart() {
  SPDLOG_DEBUG("Starting operator  |  name: '{}'", this->name());
}

void LimitSortPOp::onComplete(const CompleteMessage &) {
  if(!ctx()->isComplete() && ctx()->operatorMap().allComplete(POpRelationshipType::Producer)){
    // check if has results
    if (!result_.has_value()) {
      ctx()->notifyError("Result not produced yet");
    }

    // Project using projectColumnNames
    auto expProjectTupleSet = result_.value()->projectExist(getProjectColumnNames());
    if (!expProjectTupleSet) {
      ctx()->notifyError(expProjectTupleSet.error());
    }

    shared_ptr<Message> tupleSetMessage = make_shared<TupleSetMessage>(expProjectTupleSet.value(), name());
    ctx()->tell(tupleSetMessage);
    ctx()->notifyComplete();
  }
}

void LimitSortPOp::onTupleSet(const TupleSetMessage &message) {
  const auto inputTupleSet = makeInput(message.tuples());
  result_ = selectK(inputTupleSet);
}

void LimitSortPOp::makeArrowSelectKOptions() {
  if (!arrowSelectKOptions_.has_value()) {
    vector<arrow::compute::SortKey> arrowSortKeys;
    for (const auto &sortKey: sortKeys_) {
      const auto &name = sortKey.getName();
      auto direction = sortKey.getOrder() == plan::prephysical::ASCENDING ?
                       arrow::compute::SortOrder::Ascending : arrow::compute::SortOrder::Descending;
      arrowSortKeys.emplace_back(arrow::compute::SortKey(name, direction));
    }
    arrowSelectKOptions_ = arrow::compute::SelectKOptions(k_, arrowSortKeys);
  }
}

shared_ptr<TupleSet> LimitSortPOp::makeInput(const shared_ptr<TupleSet> &tupleSet) {
  if (!result_.has_value()) {
    return tupleSet;
  } else {
    const auto &concatenateResult = TupleSet::concatenate({result_.value(), tupleSet});
    if (!concatenateResult.has_value()) {
      ctx()->notifyError(concatenateResult.error());
    }
    return concatenateResult.value();
  }
}

shared_ptr<TupleSet> LimitSortPOp::selectK(const shared_ptr<TupleSet> &tupleSet) {
  // arrow api will crash if table has no rows, so we need a check
  if (tupleSet->numRows() == 0) {
    return tupleSet;
  }

  // Make selectKOptions if not yet
  makeArrowSelectKOptions();

  // Compute selectK indices
  const auto table = tupleSet->table();
  if (!arrowSelectKOptions_.has_value()) {
    ctx()->notifyError("Arrow SelectKOptions not set yet");
  }
  const auto &expSelectKIndices = arrow::compute::SelectKUnstable(table, *arrowSelectKOptions_);
  if (!expSelectKIndices.ok()) {
    ctx()->notifyError(expSelectKIndices.status().message());
  }
  const auto &selectKIndices = *expSelectKIndices;

  // Make result table using selectKIndices
  const auto expSelectKTable = arrow::compute::Take(table, selectKIndices);
  if (!expSelectKTable.ok()) {
    ctx()->notifyError(expSelectKTable.status().message());
  }

  return TupleSet::make((*expSelectKTable).table());
}

void LimitSortPOp::clear() {
  result_ = nullopt;
}

}
