//
// Created by matt on 1/4/20.
//

#ifndef NORMAL_NORMAL_SQL_SRC_AST_SCANNODE_H
#define NORMAL_NORMAL_SQL_SRC_AST_SCANNODE_H

#include <string>
#include <connector/Catalogue.h>
#include "ASTNode.h"
class ScanNode : public ASTNode {
public:
  std::shared_ptr<LocalFileSystemCatalogueEntry> tableName;

  std::shared_ptr<normal::core::Operator> toOperator() override;
};

#endif //NORMAL_NORMAL_SQL_SRC_AST_SCANNODE_H
