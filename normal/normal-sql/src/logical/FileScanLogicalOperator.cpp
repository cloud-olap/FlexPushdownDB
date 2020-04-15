//
// Created by matt on 7/4/20.
//

#include <normal/sql/logical/FileScanLogicalOperator.h>

#include <normal/pushdown/FileScan.h>


normal::sql::logical::FileScanLogicalOperator::FileScanLogicalOperator(std::string path) : path_(std::move(path)) {}

const std::string &normal::sql::logical::FileScanLogicalOperator::path() const {
  return path_;
}

std::shared_ptr<normal::core::Operator> normal::sql::logical::FileScanLogicalOperator::toOperator() {
  return std::make_shared<normal::pushdown::FileScan>(this->name, this->path_);
}


