//
// Created by matt on 29/4/20.
//

#include <fpdb/executor/physical/join/hashjoin/HashJoinBuildPOp.h>
#include <fpdb/executor/physical/Globals.h>
#include <fpdb/executor/message/TupleSetIndexMessage.h>
#include <fpdb/tuple/TupleSet.h>
#include <utility>

using namespace fpdb::executor::physical::join;
using namespace fpdb::tuple;

HashJoinBuildPOp::HashJoinBuildPOp(const string &name,
                                   const vector<string> &projectColumnNames,
                                   int nodeId,
                                   const vector<string> &predColumnNames) :
	PhysicalOp(name, HASH_JOIN_BUILD, projectColumnNames, nodeId),
	kernel_(HashJoinBuildKernel::make(predColumnNames)){
}

std::string HashJoinBuildPOp::getTypeString() const {
  return "HashJoinBuildPOp";
}

void HashJoinBuildPOp::onReceive(const Envelope &msg) {
  if (msg.message().type() == MessageType::START) {
	  this->onStart();
  } else if (msg.message().type() == MessageType::TUPLESET) {
    auto tupleSetMessage = dynamic_cast<const TupleSetMessage &>(msg.message());
    this->onTupleSet(tupleSetMessage);
  } else if (msg.message().type() == MessageType::COMPLETE) {
    auto completeMessage = dynamic_cast<const CompleteMessage &>(msg.message());
    this->onComplete(completeMessage);
  } else {
    ctx()->notifyError(fmt::format("Unrecognized message type: {}, {}", msg.message().getTypeString(), name()));
  }
}

void HashJoinBuildPOp::onStart() {
  SPDLOG_DEBUG("Starting operator  |  name: '{}'", this->name());
}

void HashJoinBuildPOp::onTupleSet(const TupleSetMessage &msg) {
  const auto& tupleSet = msg.tuples();

  SPDLOG_DEBUG("Adding tuple set to hash table  |  operator: '{}', tupleSet:\n{}", this->name(), tupleSet->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented, 1000)));

  auto result = buffer(tupleSet);
  if(!result)
    ctx()->notifyError(fmt::format("{}, {}", result.error(), name()));
  send(false);
}

void HashJoinBuildPOp::onComplete(const CompleteMessage &) {
  if(!ctx()->isComplete() && ctx()->operatorMap().allComplete(POpRelationshipType::Producer)) {
    send(true);
    ctx()->notifyComplete();
  }
}

tl::expected<void, string> HashJoinBuildPOp::buffer(const shared_ptr<TupleSet> &tupleSet) {
  return kernel_.put(tupleSet);
}

void HashJoinBuildPOp::send(bool force) {
  if (kernel_.getTupleSetIndex().has_value() && (force || kernel_.getTupleSetIndex().value()->size() >= DefaultBufferSize)) {
    shared_ptr<Message> message =
            make_shared<TupleSetIndexMessage>(kernel_.getTupleSetIndex().value(), name());
    ctx()->tell(message);
    kernel_.clear();
  }
}

void HashJoinBuildPOp::clear() {
  kernel_.clear();
}
