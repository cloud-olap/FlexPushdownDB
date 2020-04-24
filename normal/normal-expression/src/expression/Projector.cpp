//
// Created by matt on 21/4/20.
//

#include <gandiva/tree_expr_builder.h>
#include <gandiva/projector.h>
#include "normal/expression/Projector.h"

using namespace normal::expression;

Projector::Projector(const std::vector<std::shared_ptr<Expression>> &Expressions) : expressions_(
	Expressions) {}

void Projector::compile(std::shared_ptr<arrow::Schema> schema) {

  // Compile the expressions
  gandivaExpressions_.reserve(expressions_.size());
  for (const auto &expression: expressions_) {
	expression->compile(schema);

	gandivaExpressions_.emplace_back(gandiva::TreeExprBuilder::MakeExpression(expression->getGandivaExpression(),
																			  field(expression->name(),
																					expression->getReturnType())));
  }

  // Build a projector for the expression.
  auto status = gandiva::Projector::Make(schema,
										 gandivaExpressions_,
										 gandiva::ConfigurationBuilder::DefaultConfiguration(),
										 &gandivaProjector_);

  if(!status.ok()){
	throw std::runtime_error(status.message());
  }

  // Prepare the schema for the results
  auto resultFields = std::vector<std::shared_ptr<arrow::Field>>();
  for (const auto &e: expressions_) {
	resultFields.emplace_back(arrow::field(e->name(), e->getReturnType()));
  }
  resultSchema_ = arrow::schema(resultFields);
}

const std::vector<std::shared_ptr<Expression>> &Projector::getExpressions() const {
  return expressions_;
}

const std::shared_ptr<arrow::Schema> &Projector::getResultSchema() const {
  return resultSchema_;
}

const std::vector<gandiva::ExpressionPtr> &Projector::getGandivaExpressions() const {
  return gandivaExpressions_;
}

const std::shared_ptr<gandiva::Projector> &Projector::getGandivaProjector() const {
  return gandivaProjector_;
}
std::string Projector::showString() {
  std::stringstream ss;
  for (const auto &gandivaExpression: gandivaExpressions_) {
	ss << gandivaExpression->ToString() << std::endl;
  }
  return ss.str();
}
