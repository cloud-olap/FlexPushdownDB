//
// Created by matt on 7/4/20.
//

#ifndef NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_LOGICAL_FILESCANLOGICALOPERATOR_H
#define NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_LOGICAL_FILESCANLOGICALOPERATOR_H

#include <string>

#include <normal/core/Operator.h>

#include "ScanLogicalOperator.h"
#include <normal/connector/local-fs/LocalFilePartitioningScheme.h>

namespace normal::plan {

class FileScanLogicalOperator: public ScanLogicalOperator {

public:
  explicit FileScanLogicalOperator(const std::shared_ptr<LocalFilePartitioningScheme>& partitioningScheme);

  std::shared_ptr<core::Operator> toOperator() override;

};

}

#endif //NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_LOGICAL_FILESCANLOGICALOPERATOR_H
