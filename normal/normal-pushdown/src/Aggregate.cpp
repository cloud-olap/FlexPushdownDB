//
// Created by matt on 11/12/19.
//

#include "normal/pushdown/Aggregate.h"

#include <string>
#include <utility>
#include <memory>
#include <normal/core/CompleteMessage.h>

#include "normal/core/Operator.h"
#include "normal/core/TupleMessage.h"
#include "normal/core/Message.h"

#include "normal/pushdown/Globals.h"
#include "normal/pushdown/AggregateExpression.h"

namespace normal::pushdown {

Aggregate::Aggregate(std::string name, std::vector<std::unique_ptr<AggregateExpression>> expressions)
    : Operator(std::move(name)),
      expressions_(std::move(expressions)) {}

void Aggregate::onStart() {
  SPDLOG_DEBUG("Starting");
}

void Aggregate::onReceive(const normal::core::Envelope &message) {
  if (message.message().type() == "StartMessage") {
    this->onStart();
  } else if (message.message().type() == "TupleMessage") {
    auto tupleMessage = dynamic_cast<const normal::core::TupleMessage &>(message.message());
    this->onTuple(tupleMessage);
  } else if (message.message().type() == "CompleteMessage") {
    auto completeMessage = dynamic_cast<const normal::core::CompleteMessage &>(message.message());
    this->onComplete(completeMessage);
  } else {
    throw;
  }
}

void Aggregate::onComplete(const normal::core::CompleteMessage &message) {

  std::shared_ptr<normal::core::TupleSet> aggregateTupleSet = nullptr;

  // FIXME: Only supports one expression at mo
  for (auto &expr : expressions_) {
    aggregateTupleSet = expr->apply(inputTuples, aggregateTupleSet);
  }

  std::shared_ptr<normal::core::Message> tupleMessage = std::make_shared<normal::core::TupleMessage>(aggregateTupleSet);
  ctx()->tell(tupleMessage);

  SPDLOG_DEBUG("Completing");
  std::shared_ptr<normal::core::Message> cm = std::make_shared<normal::core::CompleteMessage>();
  ctx()->tell(cm);

  ctx()->operatorActor()->quit();
}

void Aggregate::onTuple(normal::core::TupleMessage message) {

  SPDLOG_DEBUG("Received tuple message");

  if (inputTuples == nullptr) {
    inputTuples = message.tuples();
  } else {
    auto tables = std::vector<std::shared_ptr<arrow::Table>>();
    std::shared_ptr<arrow::Table> table;
    tables.push_back(message.tuples()->table());
    tables.push_back(inputTuples->table());
    arrow::ConcatenateTables(tables, &table);
    inputTuples->table(table);
  }
}

}