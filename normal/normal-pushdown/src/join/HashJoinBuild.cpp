//
// Created by matt on 29/4/20.
//

#include "normal/pushdown/join/HashJoinBuild.h"

#include <utility>

#include <normal/pushdown/Globals.h>
#include <normal/pushdown/join/ATTIC/HashTableMessage.h>
#include <normal/tuple/TupleSet2.h>
#include <normal/pushdown/join/TupleSetIndexMessage.h>

using namespace normal::pushdown;
using namespace normal::pushdown::join;
using namespace normal::tuple;
using namespace normal::core;

HashJoinBuild::HashJoinBuild(const std::string &name, std::string columnName, long queryId) :
	Operator(name, "HashJoinBuild", queryId),
	columnName_(std::move(columnName)),
	kernel_(HashJoinBuildKernel2::make(columnName_)){
}

std::shared_ptr<HashJoinBuild> HashJoinBuild::create(const std::string &name, const std::string &columnName) {
  auto canonicalColumnName = ColumnName::canonicalize(columnName);
  return std::make_shared<HashJoinBuild>(name, canonicalColumnName);
}

void HashJoinBuild::onReceive(const normal::core::message::Envelope &msg) {
  if (msg.message().type() == "StartMessage") {
	this->onStart();
  } else if (msg.message().type() == "TupleMessage") {
	auto tupleMessage = dynamic_cast<const normal::core::message::TupleMessage &>(msg.message());
	this->onTuple(tupleMessage);
  } else if (msg.message().type() == "CompleteMessage") {
	auto completeMessage = dynamic_cast<const normal::core::message::CompleteMessage &>(msg.message());
	this->onComplete(completeMessage);
  } else {
	// FIXME: Propagate error properly
	throw std::runtime_error(fmt::format("Unrecognized message type: {}, {}", msg.message().type(), name()));
  }
}

void HashJoinBuild::onStart() {
  SPDLOG_DEBUG("Starting operator  |  name: '{}'", this->name());
//  kernel_.clear();
}

void HashJoinBuild::onTuple(const normal::core::message::TupleMessage &msg) {
  auto tupleSet = TupleSet2::create(msg.tuples());

//  SPDLOG_DEBUG("Adding tuple set to hash table  |  operator: '{}', tupleSet:\n{}", this->name(), tupleSet->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented, 1000)));

  auto startTime = std::chrono::steady_clock::now();
  auto result = kernel_.put(tupleSet);
  auto stopTime = std::chrono::steady_clock::now();
  auto time = std::chrono::duration_cast<std::chrono::nanoseconds>(stopTime - startTime).count();
  numRows_ += tupleSet->numRows();
  bytesJoinBuild_ += tupleSet->size();
  joinBuildTime_ += time;

  if(!result) throw std::runtime_error(fmt::format("{}, {}", result.error(), name()));

//  SPDLOG_DEBUG("Added tupleset to hashtable  |  Build relation hashtable:\n{}", hashtable_->toString());
}

void HashJoinBuild::onComplete(const normal::core::message::CompleteMessage &) {

  if(ctx()->operatorMap().allComplete(OperatorRelationshipType::Producer) && !hasProcessedAllComplete_) {
//	SPDLOG_DEBUG("Completing  |  Build relation hashtable:\n{}", hashtable_->toString());

//	std::shared_ptr<normal::core::message::Message>
//		hashTableMessage = std::make_shared<HashTableMessage>(kernel_.getHashTable(), name());

//	kernel_.getTupleSetIndex().value()->validate();

    double speed = (((double) bytesJoinBuild_) / 1024.0 / 1024.0) / (((double) joinBuildTime_) / 1000000000);
//    SPDLOG_INFO("JoinBuild time: {}, numBytes: {}, speed: {}MB/s, numRows: {}, {}", joinBuildTime_, bytesJoinBuild_, speed, numRows_, name());

    std::shared_ptr<normal::core::message::Message>
		message = std::make_shared<TupleSetIndexMessage>(kernel_.getTupleSetIndex().value(), name());

	ctx()->tell(message);

	ctx()->notifyComplete();
	hasProcessedAllComplete_ = true;
  }
}
