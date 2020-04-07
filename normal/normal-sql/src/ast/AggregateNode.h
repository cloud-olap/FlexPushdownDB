//
// Created by matt on 2/4/20.
//

#ifndef NORMAL_NORMAL_SQL_SRC_AST_AGGREGATENODE_H
#define NORMAL_NORMAL_SQL_SRC_AST_AGGREGATENODE_H

#include <vector>

#include "AggregateFunction.h"
#include "ASTNode.h"

class AggregateNode : public ASTNode {
private:
  std::vector<std::shared_ptr<AggregateFunction>> functions_;

public:
  explicit AggregateNode(std::vector<std::shared_ptr<AggregateFunction>> functions);

  [[nodiscard]] const std::vector<std::shared_ptr<AggregateFunction>> &functions() const;

  std::shared_ptr<normal::core::Operator> toOperator() override;
};

#endif //NORMAL_NORMAL_SQL_SRC_AST_AGGREGATENODE_H
