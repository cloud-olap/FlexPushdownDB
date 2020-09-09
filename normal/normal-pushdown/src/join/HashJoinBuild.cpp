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

HashJoinBuild::HashJoinBuild(const std::string &name, std::string columnName) :
	Operator(name, "HashJoinBuild"),
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

  auto result = kernel_.put(tupleSet);
  if(!result) throw std::runtime_error(fmt::format("{}, {}", result.error(), name()));

//  SPDLOG_DEBUG("Added tupleset to hashtable  |  Build relation hashtable:\n{}", hashtable_->toString());
}

void HashJoinBuild::onComplete(const normal::core::message::CompleteMessage &) {

  if(ctx()->operatorMap().allComplete(OperatorRelationshipType::Producer)) {
//	SPDLOG_DEBUG("Completing  |  Build relation hashtable:\n{}", hashtable_->toString());

//	std::shared_ptr<normal::core::message::Message>
//		hashTableMessage = std::make_shared<HashTableMessage>(kernel_.getHashTable(), name());

//	kernel_.getTupleSetIndex().value()->validate();

	std::shared_ptr<normal::core::message::Message>
		message = std::make_shared<TupleSetIndexMessage>(kernel_.getTupleSetIndex().value(), name());

	ctx()->tell(message);

	ctx()->notifyComplete();
  }
}
