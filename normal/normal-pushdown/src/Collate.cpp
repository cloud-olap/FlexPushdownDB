//
// Created by matt on 5/12/19.
//

#include "normal/pushdown/Collate.h"

#include <vector>                      // for vector

#include <spdlog/spdlog.h>
#include <arrow/table.h>               // for ConcatenateTables, Table (ptr ...
#include <arrow/pretty_print.h>

#include <normal/core/TupleMessage.h>

class Message;

void Collate::onStart() {
}

void Collate::onStop() {
}

Collate::Collate(std::string name) : Operator(std::move(name)) {
}

void Collate::onReceive(const normal::core::Envelope& msg) {
  spdlog::info("{}  |  Received", this->name());

  auto tupleMessage = dynamic_cast<const TupleMessage&>(msg.message());
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

void Collate::onComplete(const Operator &op) {
  ctx()->complete();
}

void Collate::show() {

  assert(m_tupleSet);

  spdlog::info("{}  |  Show:\n{}", this->name(), m_tupleSet->toString());
}

std::shared_ptr<TupleSet> Collate::tuples() {

  assert(m_tupleSet);

  return m_tupleSet;
}
