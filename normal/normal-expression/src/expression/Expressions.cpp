//
// Created by matt on 5/4/20.
//

#include <normal/expression/Expressions.h>

using namespace normal::expression;

std::shared_ptr<arrow::ArrayVector> Expressions::evaluate(const std::vector<std::shared_ptr<normal::expression::Expression>> &expressions,
														  const arrow::RecordBatch &recordBatch) {

  // Prepare a schema for the results
  auto resultFields = std::vector<std::shared_ptr<arrow::Field>>();
  for (const auto &e: expressions) {
	resultFields.emplace_back(arrow::field(e->name(), e->getReturnType()));
  }
  auto resultSchema = arrow::schema(resultFields);

  // Create gandiva expressions
  std::vector<gandiva::ExpressionPtr> gandivaExpressions;
  gandivaExpressions.reserve(expressions.size());
  for (const auto &e: expressions) {
	gandivaExpressions.emplace_back(gandiva::TreeExprBuilder::MakeExpression(e->getGandivaExpression(),
																			 field(e->name(),
																				   e->getReturnType())));
  }

  // Build a projector for the expression.
  std::shared_ptr<gandiva::Projector> projector;
  auto status = gandiva::Projector::Make(recordBatch.schema(),
										 gandivaExpressions,
										 gandiva::ConfigurationBuilder::DefaultConfiguration(),
										 &projector);

  if(!status.ok()){
	throw std::runtime_error(status.message());
  }

  // Evaluate the expressions
  auto outputs = std::make_shared<arrow::ArrayVector>();
  status = projector->Evaluate(recordBatch, arrow::default_memory_pool(), &*outputs);

  if(!status.ok()){
	throw std::runtime_error(status.message());
  }

  return outputs;
}


std::shared_ptr<arrow::ArrayVector> Expressions::evaluate(const std::shared_ptr<Projector> &projector,
														  const arrow::RecordBatch &recordBatch) {

//  // Prepare a schema for the results
//  auto resultFields = std::vector<std::shared_ptr<arrow::Field>>();
//  for (const auto &e: expressions) {
//	resultFields.emplace_back(arrow::field(e->name(), e->getReturnType()));
//  }
//  auto resultSchema = arrow::schema(resultFields);
//
//  // Create gandiva expressions
//  std::vector<gandiva::ExpressionPtr> gandivaExpressions;
//  gandivaExpressions.reserve(expressions.size());
//  for (const auto &e: expressions) {
//	gandivaExpressions.emplace_back(gandiva::TreeExprBuilder::MakeExpression(e->getGandivaExpression(),
//																			 field(e->name(),
//																				   e->getReturnType())));
//  }
//
//  // Build a projector for the expression.
//  std::shared_ptr<gandiva::Projector> projector;
//  auto status = gandiva::Projector::Make(recordBatch.schema(),
//										 gandivaExpressions,
//										 gandiva::ConfigurationBuilder::DefaultConfiguration(),
//										 &projector);
//
//  if(!status.ok()){
//	throw std::runtime_error(status.message());
//  }

  // Evaluate the expressions
  auto outputs = std::make_shared<arrow::ArrayVector>();
//  status = projector->Evaluate(recordBatch, arrow::default_memory_pool(), &*outputs);
//
//  if(!status.ok()){
//	throw std::runtime_error(status.message());
//  }

  return outputs;
}
