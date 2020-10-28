//
// Created by matt on 20/7/20.
//

#include <normal/tuple/TupleSet2.h>
#include "normal/pushdown/merge/Merge.h"
#include "normal/pushdown/merge/MergeKernel.h"

using namespace normal::pushdown::merge;

Merge::Merge(const std::string &Name, long queryId) :
	Operator(Name, "Merge", queryId) {
}

std::shared_ptr<Merge> Merge::make(const std::string &Name, long queryId) {
  return std::make_shared<Merge>(Name, queryId);
}

void Merge::onReceive(const Envelope &msg) {
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

void Merge::onStart() {

  SPDLOG_DEBUG("Starting operator  |  name: '{}', leftProducer: {}, rightProducer: {}",
			   name(),
			   leftProducer_.lock()->name(),
			   rightProducer_.lock()->name());
}

void Merge::merge() {
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
//      SPDLOG_INFO("Merge tuples: {}", name());
    }

    // Pop the processed tuple sets from the queues
    leftTupleSets_.pop_front();
    rightTupleSets_.pop_front();
  }
}

void Merge::onComplete(const CompleteMessage &) {
  if (!ctx()->isComplete() && ctx()->operatorMap().allComplete(OperatorRelationshipType::Producer)) {
	  ctx()->notifyComplete();
  }
}

void Merge::onTuple(const TupleMessage &message) {
  // Get the tuple set
  const auto &tupleSet = TupleSet2::create(message.tuples());

  // Add the tupleset to a slot in left or right producers tuple queue
  if (message.sender() == leftProducer_.lock()->name()) {
	leftTupleSets_.emplace_back(tupleSet);
  } else if (message.sender() == rightProducer_.lock()->name()) {
	rightTupleSets_.emplace_back(tupleSet);
  } else {
	throw std::runtime_error(fmt::format("Unrecognized producer {}, left: {}, right: {}",
	        message.sender(), leftProducer_.lock()->name(), rightProducer_.lock()->name()));
  }

  // Merge
  merge();
}

void Merge::setLeftProducer(const std::shared_ptr<Operator> &leftProducer) {
  leftProducer_ = leftProducer;
  consume(leftProducer);
}

void Merge::setRightProducer(const std::shared_ptr<Operator> &rightProducer) {
  rightProducer_ = rightProducer;
  consume(rightProducer);
}
