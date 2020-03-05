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
      m_expressions(std::move(expressions)) {}

void Aggregate::onStart() {
  SPDLOG_DEBUG("Starting");
}

void Aggregate::onReceive(const normal::core::Envelope &msg) {
  if (msg.message().type() == "StartMessage") {
    this->onStart();
  } else if (msg.message().type() == "TupleMessage") {
    auto tupleMessage = dynamic_cast<const normal::core::TupleMessage &>(msg.message());
    this->onTuple(tupleMessage);
  } else if (msg.message().type() == "CompleteMessage") {
    auto completeMessage = dynamic_cast<const normal::core::CompleteMessage &>(msg.message());
    this->onComplete(completeMessage);
  } else {
    Operator::onReceive(msg);
  }
}

void Aggregate::onComplete(const normal::core::CompleteMessage &msg) {

  std::shared_ptr<normal::core::TupleSet> aggregateTupleSet = nullptr;

  // FIXME: Only supports one expression at mo
  for (auto &expr : m_expressions) {
    aggregateTupleSet = expr->apply(inputTupleSet, aggregateTupleSet);
  }

  std::shared_ptr<normal::core::Message> message = std::make_shared<normal::core::TupleMessage>(aggregateTupleSet);
  ctx()->tell(message);

  SPDLOG_DEBUG("Completing");
  std::shared_ptr<normal::core::Message> cm = std::make_shared<normal::core::CompleteMessage>();
  ctx()->tell(cm);

  ctx()->getOperatorActor()->quit();
}

void Aggregate::onTuple(normal::core::TupleMessage msg) {

  SPDLOG_DEBUG("Received tuple message");

  if (inputTupleSet == nullptr) {
    inputTupleSet = msg.data();
  } else {
    auto tables = std::vector<std::shared_ptr<arrow::Table>>();
    std::shared_ptr<arrow::Table> table;
    tables.push_back(msg.data()->getTable());
    tables.push_back(inputTupleSet->getTable());
    arrow::ConcatenateTables(tables, &table);
    inputTupleSet->setTable(table);
  }
}

}