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

void Collate::onReceive(std::unique_ptr<Message> msg) {
  spdlog::info("{}  |  Received", this->name());

  std::unique_ptr<TupleMessage>
      tupleMessage = std::unique_ptr<TupleMessage>{static_cast<TupleMessage *>(msg.release())};
  if (!m_tupleSet) {
    m_tupleSet = tupleMessage->data();
  } else {
    auto tables = std::vector<std::shared_ptr<arrow::Table>>();
    std::shared_ptr<arrow::Table> table;
    tables.push_back(tupleMessage->data()->getTable());
    tables.push_back(m_tupleSet->getTable());
    arrow::ConcatenateTables(tables, &table);
    m_tupleSet->setTable(table);
  }
}

void Collate::show() {
  spdlog::info("{}  |  Show:\n{}", this->name(), m_tupleSet->toString());
}
