//
// Created by matt on 5/12/19.
//

#include "normal/pushdown/Collate.h"

#include <vector>                      // for vector


#include <arrow/table.h>               // for ConcatenateTables, Table (ptr ...
#include <arrow/pretty_print.h>

#include <normal/core/TupleMessage.h>
#include <normal/core/CompleteMessage.h>

#include "normal/pushdown/Globals.h"

void Collate::onStart() {
  SPDLOG_DEBUG("Starting");
}

Collate::Collate(std::string name) : Operator(std::move(name)) {
}

void Collate::onReceive(const normal::core::Envelope& msg) {
  if (msg.message().type() == "StartMessage") {
    this->onStart();
  } else if (msg.message().type() == "TupleMessage") {
    auto tupleMessage = dynamic_cast<const normal::core::TupleMessage &>(msg.message());
    this->onTuple(tupleMessage);
  }
  else if (msg.message().type() == "CompleteMessage") {
    auto completeMessage = dynamic_cast<const normal::core::CompleteMessage &>(msg.message());
    this->onComplete(completeMessage);
  } else {
    Operator::onReceive(msg);
  }
}

void Collate::onComplete(const normal::core::CompleteMessage& msg) {
  ctx()->getOperatorActor()->quit();
}

void Collate::show() {

  assert(m_tupleSet);

  SPDLOG_DEBUG("{}  |  Show:\n{}", this->name(), m_tupleSet->toString());
}

std::shared_ptr<normal::core::TupleSet> Collate::tuples() {

  assert(m_tupleSet);

  return m_tupleSet;
}
void Collate::onTuple(normal::core::TupleMessage tupleMessage) {

  SPDLOG_DEBUG("Received tuples");

  if (!m_tupleSet) {
    assert(tupleMessage.data());
    m_tupleSet = tupleMessage.data();
  } else {
    auto tables = std::vector<std::shared_ptr<arrow::Table>>();
    std::shared_ptr<arrow::Table> table;
    tables.push_back(tupleMessage.data()->getTable());
    tables.push_back(m_tupleSet->getTable());
    arrow::ConcatenateTables(tables, &table);
    m_tupleSet->setTable(table);
  }
}
