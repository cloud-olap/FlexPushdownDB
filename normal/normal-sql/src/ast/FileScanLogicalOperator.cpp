//
// Created by matt on 7/4/20.
//

#include <normal/pushdown/FileScan.h>
#include <connector/local-fs/LocalFileSystemCatalogueEntry.h>
#include "FileScanLogicalOperator.h"

FileScanLogicalOperator::FileScanLogicalOperator(const std::string &path) : path_(path) {}

const std::string &FileScanLogicalOperator::path() const {
  return path_;
}

std::shared_ptr<normal::core::Operator> FileScanLogicalOperator::toOperator() {
  return std::make_shared<normal::pushdown::FileScan>(this->name, this->path_);
}


