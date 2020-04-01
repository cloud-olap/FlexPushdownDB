//
// Created by matt on 1/4/20.
//

#include <normal/pushdown/FileScan.h>
#include <connector/local-fs/LocalFileSystemCatalogueEntry.h>
#include "ScanNode.h"

std::shared_ptr<normal::core::Operator> ScanNode::toOperator() {
  return std::make_shared<normal::pushdown::FileScan>(this->name, this->tableName->getPath());
}
