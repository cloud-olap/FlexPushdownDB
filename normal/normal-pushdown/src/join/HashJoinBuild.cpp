//
// Created by matt on 29/4/20.
//

#include "normal/pushdown/join/HashJoinBuild.h"

#include <utility>

#include <normal/pushdown/Globals.h>
#include <normal/pushdown/join/HashTableMessage.h>
#include <normal/tuple/TupleSet2.h>

using namespace normal::pushdown;
using namespace normal::pushdown::join;
using namespace normal::tuple;
using namespace normal::core;

HashJoinBuild::HashJoinBuild(const std::string &name, std::string columnName) :
	Operator(name, "HashJoinBuild"),
	columnName_(std::move(columnName)),
	hashtable_(std::make_shared<HashTable>()) {
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
	throw std::runtime_error("Unrecognized message type " + msg.message().type());
  }
}

void HashJoinBuild::onStart() {
  hashtable_->clear();
}

void HashJoinBuild::onTuple(const normal::core::message::TupleMessage &msg) {
  auto tupleSet = TupleSet2::create(msg.tuples());

//  SPDLOG_DEBUG("Adding tuple set to hash table  |  operator: '{}', tupleSet:\n{}", this->name(), tupleSet->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented, 1000)));

  auto putResult = hashtable_->put(columnName_, tupleSet);

  if (!putResult.has_value()) {
	throw std::runtime_error(putResult.error());
  }

//  SPDLOG_DEBUG("Added tupleset to hashtable  |  Build relation hashtable:\n{}", hashtable_->toString());
}

void HashJoinBuild::onComplete(const normal::core::message::CompleteMessage &) {

  if(ctx()->operatorMap().allComplete(OperatorRelationshipType::Producer)) {
//	SPDLOG_DEBUG("Completing  |  Build relation hashtable:\n{}", hashtable_->toString());

	std::shared_ptr<normal::core::message::Message>
		hashTableMessage = std::make_shared<HashTableMessage>(hashtable_, name());

	ctx()->tell(hashTableMessage);

	ctx()->notifyComplete();
  }
}
