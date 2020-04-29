//
// Created by matt on 29/4/20.
//

#include "normal/pushdown/join/HashJoinBuild.h"

#include <utility>

#include <normal/pushdown/Globals.h>
#include <normal/pushdown/join/HashTableMessage.h>

using namespace normal::pushdown::join;

HashJoinBuild::HashJoinBuild(const std::string &name, std::string columnName) :
	Operator(name, "HashJoinBuild"),
	columnName_(std::move(columnName)),
	hashtable_(std::make_shared<std::unordered_multimap<std::shared_ptr<arrow::Scalar>, long>>()) {
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

void HashJoinBuild::onTuple(normal::core::message::TupleMessage msg) {

  // FIXME: This should probably use record batch visitor

  std::shared_ptr<arrow::RecordBatch> batch;
  arrow::TableBatchReader reader(*msg.tuples()->table());
  reader.set_chunksize(DEFAULT_CHUNK_SIZE);
  auto arrowStatus = reader.ReadNext(&batch);

  std::shared_ptr<arrow::Scalar> result;
  while (arrowStatus.ok() && batch) {

	auto joinColumn = batch->GetColumnByName(columnName_);
	auto joinColumnType = joinColumn->type();

	if (joinColumnType->id() == arrow::int64()->id()) {

	  auto typedJoinColumn = std::static_pointer_cast<arrow::Int64Array>(joinColumn);
	  for (long r = 0; r < typedJoinColumn->length(); r++) {

		// FIXME: Is this the best way to use Arrow, get the value out and then make a scalar?

		auto value = typedJoinColumn->Value(r);
		auto scalar = arrow::MakeScalar(value);
		hashtable_->insert(std::pair<std::shared_ptr<arrow::Scalar>, long>(scalar, r));
	  }
	} else {
	  throw std::runtime_error("Join on column type '" + joinColumnType->ToString() + "' not implemented yet");
	}

	arrowStatus = reader.ReadNext(&batch);
  }
}

void HashJoinBuild::onComplete(normal::core::message::CompleteMessage) {

  std::shared_ptr<normal::core::message::Message>
	  hashTableMessage = std::make_shared<HashTableMessage>(hashtable_, name());

  ctx()->tell(hashTableMessage);

  ctx()->notifyComplete();
}

