//
// Created by matt on 27/4/20.
//

#include "normal/expression/simple/Projector.h"

using namespace normal::expression::simple;

Projector::Projector(std::vector<std::shared_ptr<Expression>> Expressions) :
	expressions_(std::move(Expressions)) {}

std::shared_ptr<arrow::Schema> Projector::getResultSchema() {
  // Prepare the schema for the results
  auto resultFields = std::vector<std::shared_ptr<arrow::Field>>();
  for (const auto &e: expressions_) {
	resultFields.emplace_back(arrow::field(e->alias(), e->getReturnType()));
  }
  return arrow::schema(resultFields);
}

std::shared_ptr<arrow::ArrayVector> Projector::evaluate(const arrow::RecordBatch &recordBatch) {

  // Evaluate the expressions
  auto outputs = std::make_shared<arrow::ArrayVector>();

  for(const auto &expression: expressions_){
    expression->evaluate(recordBatch);
  }

  return outputs;
}

void Projector::compile(const std::shared_ptr<arrow::Schema> &) {
	// NOOP
}
