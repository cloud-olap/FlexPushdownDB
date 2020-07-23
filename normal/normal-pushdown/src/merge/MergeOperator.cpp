//
// Created by matt on 20/7/20.
//

#include <normal/tuple/TupleSet2.h>
#include "normal/pushdown/merge/MergeOperator.h"
#include "normal/pushdown/merge/MergeKernel.h"

using namespace normal::pushdown::merge;

MergeOperator::MergeOperator(const std::string &Name) :
	Operator(Name, "Merge") {
}

std::shared_ptr<MergeOperator> MergeOperator::make(const std::string &Name) {
  return std::make_shared<MergeOperator>(Name);
}

void MergeOperator::onReceive(const Envelope &msg) {
  if (msg.message().type() == "StartMessage") {
	this->onStart();
  } else if (msg.message().type() == "TupleMessage") {
	auto tupleMessage = dynamic_cast<const TupleMessage &>(msg.message());
	this->onTuple(tupleMessage);
  } else if (msg.message().type() == "CompleteMessage") {
	auto completeMessage = dynamic_cast<const CompleteMessage &>(msg.message());
	this->onComplete(completeMessage);
  } else {
	// FIXME: Propagate error properly
	throw std::runtime_error("Unrecognized message type " + msg.message().type());
  }
}

void MergeOperator::onStart() {

  auto producers_ = this->ctx()->operatorMap().get(OperatorRelationshipType::Producer);

  if (producers_.size() < 2)
	throw std::runtime_error("Left and right producer not set");

  leftProducer_ = producers_[0];
  rightProducer_ = producers_[1];

  SPDLOG_DEBUG("Starting operator  |  name: '{}', leftProducer: {}, rightProducer: {}",
			   name(),
			   leftProducer_->name(),
			   rightProducer_->name());
}

void MergeOperator::onComplete(const CompleteMessage &) {
  if (ctx()->operatorMap().allComplete(OperatorRelationshipType::Producer)) {
	ctx()->notifyComplete();
  }
}

void MergeOperator::onTuple(const TupleMessage &message) {

  // Get the tuple set
  const auto &tupleSet = TupleSet2::create(message.tuples());

  // Add the tupleset to a slot in left or right producers tuple queue
  if (message.sender() == leftProducer_->name()) {
	leftTupleSets_.emplace_back(tupleSet);
  } else if (message.sender() == rightProducer_->name()) {
	rightTupleSets_.emplace_back(tupleSet);
  } else {
	throw std::runtime_error(fmt::format("Unrecognized producer {}", message.sender()));
  }

  // Check if we have merge-able tuple sets
  while (!leftTupleSets_.empty() && !rightTupleSets_.empty()) {

    // Take next left and right tuplesets from the queues
	auto leftTupleSet = leftTupleSets_.front();
	auto rightTupleSet = rightTupleSets_.front();

	// Merge tuplesets
	auto expectedMergedTupleSet = MergeKernel::merge(leftTupleSet, rightTupleSet);

	if (!expectedMergedTupleSet.has_value()) {
	  throw std::runtime_error(expectedMergedTupleSet.error());
	} else {
	  // Send merged tupleset
	  auto mergedTupleSet = expectedMergedTupleSet.value();
	  std::shared_ptr<core::message::Message>
		  tupleMessage = std::make_shared<core::message::TupleMessage>(mergedTupleSet->toTupleSetV1(), name());
	  ctx()->tell(tupleMessage);
	}

	// Pop the processed tuple sets from the queues
	leftTupleSets_.pop_front();
	rightTupleSets_.pop_front();
  }

}
