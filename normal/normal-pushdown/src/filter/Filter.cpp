//
// Created by matt on 6/5/20.
//

#include "normal/pushdown/filter/Filter.h"

#include <utility>

#include <normal/pushdown/TupleMessage.h>
#include <normal/core/message/CompleteMessage.h>
#include <normal/pushdown/Globals.h>
#include <normal/expression/gandiva/Filter.h>
#include <normal/tuple/TupleSetShowOptions.h>
#include <normal/tuple/Globals.h>

using namespace normal::pushdown::filter;

Filter::Filter(std::string Name, std::shared_ptr<FilterPredicate> Pred) :
	Operator(std::move(Name), "Filter"),
	pred_(std::move(Pred)),
	received_(normal::tuple::TupleSet2::make()),
	filtered_(normal::tuple::TupleSet2::make()){}

std::shared_ptr<Filter> Filter::make(std::string Name, std::shared_ptr<FilterPredicate> Pred) {
  return std::make_shared<Filter>(Name, Pred);
}

void Filter::onReceive(const normal::core::message::Envelope &Envelope) {

  const auto& message = Envelope.message();

  if (message.type() == "StartMessage") {
	this->onStart();
  } else if (message.type() == "TupleMessage") {
	auto tupleMessage = dynamic_cast<const normal::core::message::TupleMessage &>(message);
	this->onTuple(tupleMessage);
  } else if (message.type() == "CompleteMessage") {
	auto completeMessage = dynamic_cast<const normal::core::message::CompleteMessage &>(message);
	this->onComplete(completeMessage);
  } else {
	// FIXME: Propagate error properly
	throw std::runtime_error("Unrecognized message type " + message.type());
  }
}

void Filter::onStart() {
  received_->clear();
  assert(received_->validate());
  filtered_->clear();
  assert(filtered_->validate());
}

void Filter::onTuple(const normal::core::message::TupleMessage &Message) {
  bufferTuples(Message);
  buildFilter();
  if (received_->numRows() > DefaultBufferSize) {
	filterTuples();
	sendTuples();
  }
}

void Filter::onComplete(const normal::core::message::CompleteMessage&) {
  filterTuples();
  sendTuples();
  ctx()->notifyComplete();
}

void Filter::bufferTuples(const normal::core::message::TupleMessage &Message) {
  auto tupleSet = normal::tuple::TupleSet2::create(Message.tuples());
  if(!received_->schema().has_value()) {
	received_->setSchema(*tupleSet->schema());
  }
  received_->append(tupleSet);
  assert(received_->validate());
}

void Filter::buildFilter() {
  if(!filter_.has_value()){
    auto predicateExpression = pred_->expression();
	filter_ = normal::expression::gandiva::Filter::make(pred_->expression());
	filter_.value()->compile(received_->schema().value());
  }
}

void Filter::filterTuples() {

  SPDLOG_DEBUG("Filter Input\n{}", received_->showString(normal::tuple::TupleSetShowOptions(normal::tuple::TupleSetShowOrientation::RowOriented, 100)));

  filtered_ = filter_.value()->evaluate(*received_);
  assert(filtered_->validate());

  SPDLOG_DEBUG("Filter Output\n{}", filtered_->showString(normal::tuple::TupleSetShowOptions(normal::tuple::TupleSetShowOrientation::RowOriented, 100)));

  received_->clear();
  assert(received_->validate());
}

void Filter::sendTuples() {
  std::shared_ptr<core::message::Message> tupleMessage =
	  std::make_shared<core::message::TupleMessage>(filtered_->toTupleSetV1(), name());

  ctx()->tell(tupleMessage);
  filtered_->clear();
  assert(filtered_->validate());
}
