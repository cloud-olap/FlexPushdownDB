//
// Created by matt on 1/4/20.
//

#ifndef NORMAL_NORMAL_SQL_SRC_AST_SCANNODE_H
#define NORMAL_NORMAL_SQL_SRC_AST_SCANNODE_H

#include <string>
#include "logical/LogicalOperator.h"

class ScanLogicalOperator : public LogicalOperator {

public:
  ~ScanLogicalOperator() override = default;

  std::shared_ptr<normal::core::Operator> toOperator() override = 0;
};

#endif //NORMAL_NORMAL_SQL_SRC_AST_SCANNODE_H
