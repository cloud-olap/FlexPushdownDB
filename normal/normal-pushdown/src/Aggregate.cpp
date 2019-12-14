//
// Created by matt on 11/12/19.
//

#include "normal/pushdown/Aggregate.h"

#include <string>
#include <utility>
#include <memory>

#include "normal/core/Operator.h"
#include "normal/core/TupleMessage.h"
#include "normal/core/Message.h"
#include "normal/pushdown/AggregateExpression.h"

Aggregate::Aggregate(std::string name, std::vector<std::unique_ptr<AggregateExpression>> expressions)
    : Operator(std::move(name)) {
  m_expressions = std::move(expressions);
}

void Aggregate::onStart() {

}

void Aggregate::onStop() {

}

void Aggregate::onReceive(std::unique_ptr<Message> msg) {

  std::unique_ptr<TupleMessage>
      tupleMessage = std::unique_ptr<TupleMessage>{dynamic_cast<TupleMessage *>(msg.release())};
  auto table = tupleMessage->data();

  for (auto &expr : m_expressions) {
    table = expr->apply(table);
  }

  auto outMessage = std::make_unique<TupleMessage>(table);
  ctx()->tell(std::move(outMessage));
}

