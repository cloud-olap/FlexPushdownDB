//
// Created by matt on 20/7/20.
//

#include <fpdb/executor/physical/merge/MergePOp.h>
#include <fpdb/executor/physical/merge/MergeKernel.h>

using namespace fpdb::executor::physical::merge;

MergePOp::MergePOp(const std::string &name,
                   const std::vector<std::string> &projectColumnNames,
                   int nodeId) :
	PhysicalOp(name, MERGE, projectColumnNames, nodeId) {
}

std::string MergePOp::getTypeString() const {
  return "MergePOp";
}

void MergePOp::onReceive(const Envelope &msg) {
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

void MergePOp::onStart() {
  SPDLOG_DEBUG("Starting operator  |  name: '{}', leftProducer: {}, rightProducer: {}",
               name(),
               leftProducerName_,
               rightProducerName_);
}

void MergePOp::merge() {
  // Check if we have merge-able tuple sets
  while (!leftTupleSets_.empty() && !rightTupleSets_.empty()) {

    // Take next left and right tuplesets from the queues
    auto leftTupleSet = leftTupleSets_.front();
    auto rightTupleSet = rightTupleSets_.front();

    // Merge tuplesets
    auto expectedMergedTupleSet = MergeKernel::merge(leftTupleSet, rightTupleSet);

    if (!expectedMergedTupleSet.has_value()) {
      ctx()->notifyError(fmt::format("{}.\n leftOp: {}\n rightOp: {}",
                                expectedMergedTupleSet.error(), leftProducerName_, rightProducerName_));
    } else {
      // Send merged tupleset
      auto mergedTupleSet = expectedMergedTupleSet.value();

      // Project using projectColumnNames
      auto expProjectTupleSet = mergedTupleSet->projectExist(getProjectColumnNames());
      if (!expProjectTupleSet) {
        ctx()->notifyError(expProjectTupleSet.error());
      }

      std::shared_ptr<Message> tupleSetMessage = std::make_shared<TupleSetMessage>(expProjectTupleSet.value(), name());
      ctx()->tell(tupleSetMessage);
    }

    // Pop the processed tuple sets from the queues
    leftTupleSets_.pop_front();
    rightTupleSets_.pop_front();
  }
}

void MergePOp::onComplete(const CompleteMessage &) {
  if (!ctx()->isComplete() && ctx()->operatorMap().allComplete(POpRelationshipType::Producer)) {
	  ctx()->notifyComplete();
  }
}

void MergePOp::onTupleSet(const TupleSetMessage &message) {
  // Get the tuple set
  const auto &tupleSet = message.tuples();

  // Add the tupleset to a slot in left or right producers tuple queue
  if (message.sender() == leftProducerName_) {
	leftTupleSets_.emplace_back(tupleSet);
  } else if (message.sender() == rightProducerName_) {
	rightTupleSets_.emplace_back(tupleSet);
  } else {
	ctx()->notifyError(fmt::format("Unrecognized producer {}, left: {}, right: {}",
	        message.sender(), leftProducerName_, rightProducerName_));
  }

  // Merge
  merge();
}

void MergePOp::setLeftProducer(const std::shared_ptr<PhysicalOp> &leftProducer) {
  leftProducerName_ = leftProducer->name();
  consume(leftProducer);
}

void MergePOp::setRightProducer(const std::shared_ptr<PhysicalOp> &rightProducer) {
  rightProducerName_ = rightProducer->name();
  consume(rightProducer);
}

void MergePOp::clear() {
  leftTupleSets_.clear();
  rightTupleSets_.clear();
}
