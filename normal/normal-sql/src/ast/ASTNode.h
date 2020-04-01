//
// Created by matt on 1/4/20.
//

#ifndef NORMAL_NORMAL_SQL_SRC_AST_ASTNODE_H
#define NORMAL_NORMAL_SQL_SRC_AST_ASTNODE_H

#include <normal/core/Operator.h>

class ASTNode {
public:
  std::string name;
  std::shared_ptr<ASTNode> consumer;
  virtual std::shared_ptr<normal::core::Operator> toOperator() = 0;

};

#endif //NORMAL_NORMAL_SQL_SRC_AST_ASTNODE_H
