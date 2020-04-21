//
// Created by matt on 21/4/20.
//

#include "normal/expression/Projector.h"
normal::expression::Projector::Projector(const std::vector<std::shared_ptr<Expression>> &Expressions) : expressions_(
	Expressions) {}

void normal::expression::Projector::compile(std::shared_ptr<arrow::Schema> schema) {
  for(const auto& expression: expressions_){
	expression->compile(schema);
  }
}
