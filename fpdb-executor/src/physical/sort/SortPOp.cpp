//
// Created by Yifei Yang on 11/20/21.
//

#include <fpdb/executor/physical/sort/SortPOp.h>

namespace fpdb::executor::physical::sort {

SortPOp::SortPOp(const string &name,
                 const vector<string> &projectColumnNames,
                 int nodeId,
                 const vector<SortKey> &sortKeys) :
  PhysicalOp(name, SORT, projectColumnNames, nodeId),
  sortKeys_(sortKeys) {}

std::string SortPOp::getTypeString() const {
  return "SortPOp";
}

void SortPOp::onReceive(const Envelope &message) {
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

void SortPOp::onStart() {
  SPDLOG_DEBUG("Starting operator  |  name: '{}'", this->name());
}

void SortPOp::onComplete(const CompleteMessage &) {
  if(!ctx()->isComplete() && ctx()->operatorMap().allComplete(POpRelationshipType::Producer)){
    const auto &sortedTupleSet = sort();

    // Project using projectColumnNames
    auto expProjectTupleSet = sortedTupleSet->projectExist(getProjectColumnNames());
    if (!expProjectTupleSet) {
      throw std::runtime_error(expProjectTupleSet.error());
    }

    shared_ptr<Message> tupleMessage = make_shared<TupleSetMessage>(expProjectTupleSet.value(), name());
    ctx()->tell(tupleMessage);
    ctx()->notifyComplete();
  }
}

void SortPOp::onTupleSet(const TupleSetMessage &message) {
  buffer(message.tuples());
}

void SortPOp::makeArrowSortOptions() {
  if (!arrowSortOptions_.has_value()) {
    vector<arrow::compute::SortKey> arrowSortKeys;
    for (const auto &sortKey: sortKeys_) {
      const auto &name = sortKey.getName();
      auto direction = sortKey.getOrder() == plan::prephysical::ASCENDING ?
                       arrow::compute::SortOrder::Ascending : arrow::compute::SortOrder::Descending;
      arrowSortKeys.emplace_back(arrow::compute::SortKey(name, direction));
    }
    arrowSortOptions_ = arrow::compute::SortOptions(arrowSortKeys);
  }
}

void SortPOp::buffer(const shared_ptr<TupleSet> &tupleSet) {
  if (!buffer_.has_value()) {
    buffer_ = tupleSet;
  } else {
    const auto &concatenateResult = TupleSet::concatenate({buffer_.value(), tupleSet});
    if (!concatenateResult.has_value()) {
      ctx()->notifyError(concatenateResult.error());
    }
    buffer_ = concatenateResult.value();
  }
}

shared_ptr<TupleSet> SortPOp::sort() {
  if (!buffer_.has_value()) {
    return TupleSet::makeWithEmptyTable();
  }

  // Make sortOptions if not yet
  makeArrowSortOptions();

  // Check input buffer, arrow api will throw error if table has no rows, so we need a check
  const auto table = (*buffer_)->table();
  if (!arrowSortOptions_.has_value()) {
    ctx()->notifyError("Arrow SortOptions not set yet");
  }
  if (table->num_rows() == 0) {
    return *buffer_;
  }

  // Compute sort indices
  const auto &expSortIndices = arrow::compute::SortIndices(table, *arrowSortOptions_);
  if (!expSortIndices.ok()) {
    ctx()->notifyError(expSortIndices.status().message());
  }
  const auto &sortIndices = *expSortIndices;

  // Make sorted table using sort indices
  const auto expSortedTable = arrow::compute::Take(table, sortIndices);
  if (!expSortedTable.ok()) {
    ctx()->notifyError(expSortedTable.status().message());
  }

  return TupleSet::make((*expSortedTable).table());
}

void SortPOp::clear() {
  buffer_ = nullopt;
}

}
