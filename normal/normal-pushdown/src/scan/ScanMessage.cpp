//
// Created by matt on 21/7/20.
//

#include "normal/pushdown/scan/ScanMessage.h"

using namespace normal::pushdown::scan;

ScanMessage::ScanMessage(const std::vector<std::string> &ColumnNames, const std::string &Sender) :
	Message("ScanMessage", Sender),
	columnNames_(ColumnNames) {}

const std::vector<std::string> &ScanMessage::getColumnNames() const {
  return columnNames_;
}
