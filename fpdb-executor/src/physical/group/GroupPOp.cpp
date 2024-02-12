//
// Created by matt on 13/5/20.
//

#include <fpdb/executor/physical/group/GroupPOp.h>
#include <fpdb/executor/physical/group/GroupKernel.h>
#include <fpdb/executor/physical/group/GroupArrowKernel.h>
#include <fpdb/executor/physical/Globals.h>

using namespace fpdb::tuple;

namespace fpdb::executor::physical::group {

GroupPOp::GroupPOp(const string &name,
                   const vector<string> &projectColumnNames,
                   int nodeId,
                   const vector<string> &groupColumnNames,
                   const vector<shared_ptr<aggregate::AggregateFunction>> &aggregateFunctions) :
	PhysicalOp(name, GROUP, projectColumnNames, nodeId) {
  if (USE_ARROW_GROUP_BY_IMPL) {
    kernel_ = std::make_shared<GroupArrowKernel>(groupColumnNames, aggregateFunctions);
  } else {
    kernel_ = std::make_shared<GroupKernel>(groupColumnNames, aggregateFunctions);
  }
}

GroupPOp::GroupPOp(const string &name,
                   const vector<string> &projectColumnNames,
                   int nodeId,
                   const std::shared_ptr<GroupAbstractKernel> &kernel) :
  PhysicalOp(name, GROUP, projectColumnNames, nodeId),
  kernel_(kernel) {}

std::string GroupPOp::getTypeString() const {
  return "GroupPOp";
}

const std::shared_ptr<GroupAbstractKernel> &GroupPOp::getKernel() const {
  return kernel_;
}

void GroupPOp::onReceive(const Envelope &msg) {
  if (msg.message().type() == MessageType::START) {
	  this->onStart();
  } else if (msg.message().type() == MessageType::TUPLESET) {
    auto tupleSetMessage = dynamic_cast<const TupleSetMessage &>(msg.message());
    this->onTupleSet(tupleSetMessage);
  } else if (msg.message().type() == MessageType::COMPLETE) {
    auto completeMessage = dynamic_cast<const CompleteMessage &>(msg.message());
    this->onComplete(completeMessage);
  } else {
    ctx()->notifyError("Unrecognized message type " + msg.message().getTypeString());
  }
}

void GroupPOp::onStart() {
  SPDLOG_DEBUG("Starting operator  |  name: '{}'", this->name());
}

void GroupPOp::onTupleSet(const TupleSetMessage &message) {
  const auto &tupleSet = message.tuples();
  auto expectedGroupResult = kernel_->group(tupleSet);
  if(!expectedGroupResult)
    ctx()->notifyError(expectedGroupResult.error());
}

void GroupPOp::onComplete(const CompleteMessage &) {
  if (!ctx()->isComplete() && this->ctx()->operatorMap().allComplete(POpRelationshipType::Producer)) {

    auto expectedGroupedTupleSet = kernel_->finalise();
    if (!expectedGroupedTupleSet)
      ctx()->notifyError(expectedGroupedTupleSet.error());

    // Project using projectColumnNames
    auto expProjectTupleSet = expectedGroupedTupleSet.value()->projectExist(getProjectColumnNames());
    if (!expProjectTupleSet) {
      ctx()->notifyError(expProjectTupleSet.error());
    }

    shared_ptr<Message> tupleSetMessage = make_shared<TupleSetMessage>(expProjectTupleSet.value(), this->name());
    ctx()->tell(tupleSetMessage);

    ctx()->notifyComplete();
  }
}

void GroupPOp::clear() {
  kernel_->clear();
}

}
