//
// Created by matt on 1/4/20.
//

#ifndef NORMAL_NORMAL_SQL_SRC_AST_COLLATENODE_H
#define NORMAL_NORMAL_SQL_SRC_AST_COLLATENODE_H

#include <memory>
#include <normal/core/Operator.h>
#include "ASTNode.h"

class CollateNode : public ASTNode {
public:
  std::shared_ptr<normal::core::Operator> toOperator() override;
};

#endif //NORMAL_NORMAL_SQL_SRC_AST_COLLATENODE_H
