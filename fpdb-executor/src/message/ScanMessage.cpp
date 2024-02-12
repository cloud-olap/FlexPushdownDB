//
// Created by matt on 21/7/20.
//

#include <fpdb/executor/message/ScanMessage.h>
#include <utility>

namespace fpdb::executor::message {

ScanMessage::ScanMessage(const std::vector<std::string> &scanColumnNames,
                         const std::vector<std::string> &projectColumnNames,
                         const std::string &sender,
                         bool resultNeeded) :
	Message(SCAN, sender),
  scanColumnNames_(scanColumnNames),
  projectColumnNames_(projectColumnNames),
	resultNeeded_(resultNeeded) {}

std::string ScanMessage::getTypeString() const {
  return "ScanMessage";
}

const std::vector<std::string> &ScanMessage::getScanColumnNames() const {
  return scanColumnNames_;
}

const std::vector<std::string> &ScanMessage::getProjectColumnNames() const {
  return projectColumnNames_;
}

bool ScanMessage::isResultNeeded() const {
  return resultNeeded_;
}

}
