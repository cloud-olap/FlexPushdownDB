//
// Created by matt on 21/7/20.
//

#include "normal/pushdown/scan/ScanMessage.h"

#include <utility>

using namespace normal::pushdown::scan;

ScanMessage::ScanMessage(std::vector<std::string> ColumnNames, const std::string &Sender, bool resultNeeded) :
	Message("ScanMessage", Sender),
	columnNames_(std::move(ColumnNames)),
	resultNeeded_(resultNeeded) {}

const std::vector<std::string> &ScanMessage::getColumnNames() const {
  return columnNames_;
}

bool ScanMessage::isResultNeeded() const {
  return resultNeeded_;
}
