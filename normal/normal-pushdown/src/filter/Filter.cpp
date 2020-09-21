//
// Created by matt on 6/5/20.
//

#include "normal/pushdown/filter/Filter.h"

#include <utility>

#include <normal/pushdown/TupleMessage.h>
#include <normal/core/message/CompleteMessage.h>
#include <normal/pushdown/Globals.h>
#include <normal/expression/gandiva/Filter.h>
#include <normal/tuple/Globals.h>
#include <normal/core/cache/WeightRequestMessage.h>
#include <normal/expression/gandiva/And.h>
#include <normal/expression/gandiva/Or.h>

using namespace normal::pushdown::filter;
using namespace normal::core;
using namespace normal::cache;

Filter::Filter(std::string Name, std::shared_ptr<FilterPredicate> Pred, long queryId,
               const std::shared_ptr<std::vector<std::shared_ptr<normal::cache::SegmentKey>>> &weightedSegmentKeys) :
	Operator(std::move(Name), "Filter", queryId),
	received_(normal::tuple::TupleSet2::make()),
	filtered_(normal::tuple::TupleSet2::make()),
	pred_(Pred),
	weightedSegmentKeys_(weightedSegmentKeys) {}

std::shared_ptr<Filter> Filter::make(const std::string &Name, const std::shared_ptr<FilterPredicate> &Pred, long queryId,
                                     std::shared_ptr<std::vector<std::shared_ptr<normal::cache::SegmentKey>>> weightedSegmentKeys) {
  return std::make_shared<Filter>(Name, Pred, queryId, weightedSegmentKeys);
}

void Filter::onReceive(const normal::core::message::Envelope &Envelope) {

  const auto& message = Envelope.message();

  if (message.type() == "StartMessage") {
	this->onStart();
  } else if (message.type() == "TupleMessage") {
	auto tupleMessage = dynamic_cast<const normal::core::message::TupleMessage &>(message);
	this->onTuple(tupleMessage);
  } else if (message.type() == "CompleteMessage") {
    if (*applicable_) {
      auto completeMessage = dynamic_cast<const normal::core::message::CompleteMessage &>(message);
      this->onComplete(completeMessage);
    }
  } else {
	// FIXME: Propagate error properly
	throw std::runtime_error("Unrecognized message type " + message.type());
  }
}

void Filter::onStart() {
//  received_->clear();
  assert(received_->validate());
//  filtered_->clear();
  assert(filtered_->validate());
}

void Filter::onTuple(const normal::core::message::TupleMessage &Message) {
//  SPDLOG_DEBUG("onTuple  |  Message tupleSet - numRows: {}", Message.tuples()->numRows());
  /**
   * Check if this filter is applicable, if not, just send an empty table and complete
   */
  auto tupleSet = normal::tuple::TupleSet2::create(Message.tuples());
  if (applicable_ == nullptr) {
    applicable_ = std::make_shared<bool>(isApplicable(tupleSet));
  }

  if (*applicable_) {
    bufferTuples(tupleSet);
//    SPDLOG_INFO("Filter onTuple: {}, {}, {}", tupleSet->numRows(), received_->numRows(), name());
    buildFilter();
    if (received_->numRows() > DefaultBufferSize) {
      filterTuples();
      sendTuples();
    }
  } else {
    // empty table
    auto emptyTupleSet = normal::tuple::TupleSet2::make2();
    std::shared_ptr<core::message::Message> tupleMessage =
            std::make_shared<core::message::TupleMessage>(emptyTupleSet->toTupleSetV1(), name());
    ctx()->tell(tupleMessage);
    // complete
    std::shared_ptr<core::message::Message> completeMessage =
            std::make_shared<core::message::CompleteMessage>(name());
    ctx()->tell(completeMessage);
    ctx()->notifyComplete();
  }
}

void Filter::onComplete(const normal::core::message::CompleteMessage&) {
//  SPDLOG_DEBUG("onComplete  |  Received buffer tupleSet - numRows: {}", received_->numRows());

  if(received_->getArrowTable().has_value()) {
    filterTuples();
    sendTuples();
  }

  if(ctx()->operatorMap().allComplete(OperatorRelationshipType::Producer)){
    if (weightedSegmentKeys_ && totalNumRows_ > 0 && *applicable_) {
      sendSegmentWeight();
    }

    filter_ = std::nullopt;
    assert(this->received_->numRows() == 0);
	  assert(this->filtered_->numRows() == 0);

	  ctx()->notifyComplete();
  }
}

void Filter::bufferTuples(std::shared_ptr<normal::tuple::TupleSet2> tupleSet) {
  if(!received_->schema().has_value()) {
	received_->setSchema(*tupleSet->schema());
  }
  auto result = received_->append(tupleSet);
  if(!result.has_value()){
    throw std::runtime_error(result.error());
  }
  assert(received_->validate());
}

bool Filter::isApplicable(std::shared_ptr<normal::tuple::TupleSet2> tupleSet) {
  auto predicateColumnNames = pred_->expression()->involvedColumnNames();
  auto tupleColumnNames = std::make_shared<std::vector<std::string>>();
  for (auto const &field: tupleSet->schema()->get()->fields()) {
    tupleColumnNames->emplace_back(field->name());
  }

  for (auto const &columnName: *predicateColumnNames) {
    if (std::find(tupleColumnNames->begin(), tupleColumnNames->end(), columnName) == tupleColumnNames->end()) {
      return false;
    }
  }
  return true;
}

void Filter::buildFilter() {
  if(!filter_.has_value()){
    auto predicateExpression = pred_->expression();
	filter_ = normal::expression::gandiva::Filter::make(pred_->expression());
	filter_.value()->compile(received_->schema().value());
  }
}

void Filter::filterTuples() {

//  SPDLOG_DEBUG("Filter Input\n{}", received_->showString(normal::tuple::TupleSetShowOptions(normal::tuple::TupleSetShowOrientation::RowOriented, 100)));

  filtered_ = filter_.value()->evaluate(*received_);
  assert(filtered_->validate());

  totalNumRows_ += received_->numRows();
  filteredNumRows_ += filtered_->numRows();
//  SPDLOG_DEBUG("Filter Output\n{}", filtered_->showString(normal::tuple::TupleSetShowOptions(normal::tuple::TupleSetShowOrientation::RowOriented, 100)));

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

int getPredicateNum(const std::shared_ptr<normal::expression::gandiva::Expression> &expr) {
  if (typeid(*expr) == typeid(normal::expression::gandiva::And) || typeid(*expr) == typeid(normal::expression::gandiva::Or)) {
    auto biExpr = std::static_pointer_cast<normal::expression::gandiva::BinaryExpression>(expr);
    return getPredicateNum(biExpr->getLeft()) + getPredicateNum(biExpr->getRight());
  } else {
    return 1;
  }
}

void Filter::sendSegmentWeight() {
  double predicateNum = (double) getPredicateNum(pred_->expression());
  double predPara = 0.5;

  double weight = ((double) filteredNumRows_) / ((double ) totalNumRows_) * (predicateNum / (predicateNum + predPara));
  //double weight = ((double) filteredNumRows_) / ((double ) totalNumRows_) * (predicateNum);
  ctx()->send(core::cache::WeightRequestMessage::make(weightedSegmentKeys_, weight, getQueryId(), name()), "SegmentCache")
          .map_error([](auto err) { throw std::runtime_error(err); });
}
