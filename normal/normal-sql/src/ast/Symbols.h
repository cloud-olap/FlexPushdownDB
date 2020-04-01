//
// Created by matt on 1/4/20.
//

#ifndef NORMAL_NORMAL_SQL_SRC_AST_SYMBOLS_H
#define NORMAL_NORMAL_SQL_SRC_AST_SYMBOLS_H

#include <stddef.h>
#include <unordered_map>
#include <memory>
#include "ASTNode.h"
class Symbols {
public:
  std::unordered_map <size_t, std::shared_ptr<ASTNode>> table;
};

#endif //NORMAL_NORMAL_SQL_SRC_AST_SYMBOLS_H
