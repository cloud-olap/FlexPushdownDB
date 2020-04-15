//
// Created by matt on 1/4/20.
//

#ifndef NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_LOGICAL_SCANLOGICALOPERATOR_H
#define NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_LOGICAL_SCANLOGICALOPERATOR_H

#include <memory>

#include <normal/core/Operator.h>

#include "LogicalOperator.h"

namespace normal::plan {

class ScanLogicalOperator : public normal::plan::LogicalOperator {

public:
  ~ScanLogicalOperator() override = default;

  std::shared_ptr<core::Operator> toOperator() override = 0;
};

}

#endif //NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_LOGICAL_SCANLOGICALOPERATOR_H
