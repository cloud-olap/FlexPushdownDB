//
// Created by matt on 7/4/20.
//

#include "normal/sql/logical/FileScanLogicalOperator.h"

#include <normal/pushdown/FileScan.h>

#include <utility>

FileScanLogicalOperator::FileScanLogicalOperator(std::string path) : path_(std::move(path)) {}

const std::string &FileScanLogicalOperator::path() const {
  return path_;
}

std::shared_ptr<normal::core::Operator> FileScanLogicalOperator::toOperator() {
  return std::make_shared<normal::pushdown::FileScan>(this->name, this->path_);
}


