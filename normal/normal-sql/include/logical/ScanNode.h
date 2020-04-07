//
// Created by matt on 1/4/20.
//

#ifndef NORMAL_NORMAL_SQL_SRC_AST_SCANNODE_H
#define NORMAL_NORMAL_SQL_SRC_AST_SCANNODE_H

#include <string>
#include "../../src/ast/ASTNode.h"

class ScanNode : public ASTNode {

public:
  virtual ~ScanNode() = default;

  virtual std::shared_ptr<normal::core::Operator> toOperator() = 0;
};

#endif //NORMAL_NORMAL_SQL_SRC_AST_SCANNODE_H
