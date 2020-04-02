//
// Created by matt on 2/4/20.
//

#ifndef NORMAL_NORMAL_SQL_SRC_AST_AGGREGATEEXPRESSION_H
#define NORMAL_NORMAL_SQL_SRC_AST_AGGREGATEEXPRESSION_H

#include <string>
class AggregateExpression {
public:
  AggregateExpression(std::string Text);
  std::string text;
};

#endif //NORMAL_NORMAL_SQL_SRC_AST_AGGREGATEEXPRESSION_H
