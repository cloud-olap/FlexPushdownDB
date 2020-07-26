//
// Created by matt on 27/4/20.
//

#include "normal/expression/gandiva/Projector.h"

#include <gandiva/tree_expr_builder.h>
#include <normal/tuple/TupleSet.h>

using namespace normal::expression::gandiva;

Projector::Projector(std::vector<std::shared_ptr<Expression>> Expressions) :
	expressions_(std::move(Expressions)) {}

void Projector::compile(const std::shared_ptr<arrow::Schema> &schema) {

  // Compile the expressions
  gandivaExpressions_.reserve(expressions_.size());
  for (const auto &expression: expressions_) {
	expression->compile(schema);

	auto gandivaExpression = ::gandiva::TreeExprBuilder::MakeExpression(expression->getGandivaExpression(),
																		field(expression->alias(),
																			  expression->getReturnType()));

	SPDLOG_DEBUG(fmt::format("Gandiva expression: {}", gandivaExpression->ToString()));

	gandivaExpressions_.emplace_back(gandivaExpression);
  }

  // Build a projector for the expression.
  auto status = ::gandiva::Projector::Make(schema,
										   gandivaExpressions_,
										   ::gandiva::ConfigurationBuilder::DefaultConfiguration(),
										   &gandivaProjector_);

  if (!status.ok()) {
	throw std::runtime_error(status.message());
  }

  // Prepare the schema for the results
  auto resultFields = std::vector<std::shared_ptr<arrow::Field>>();
  for (const auto &e: expressions_) {
	resultFields.emplace_back(arrow::field(e->alias(), e->getReturnType()));
  }
  resultSchema_ = arrow::schema(resultFields);
}

std::shared_ptr<arrow::Schema> Projector::getResultSchema() {
  return resultSchema_;
}

std::string Projector::showString() {
  std::stringstream ss;
  for (const auto &gandivaExpression: gandivaExpressions_) {
	ss << gandivaExpression->ToString() << std::endl;
  }
  return ss.str();
}

std::shared_ptr<arrow::ArrayVector> Projector::evaluate(const arrow::RecordBatch &recordBatch) {

  // Evaluate the expressions
  auto outputs = std::make_shared<arrow::ArrayVector>();
  auto status = gandivaProjector_->Evaluate(recordBatch, arrow::default_memory_pool(), outputs.get());

  if (!status.ok()) {
	throw std::runtime_error(status.message());
  }

  return outputs;
}

std::shared_ptr<TupleSet> Projector::evaluate(const TupleSet &tupleSet) {

  // Read the table in batches
  std::shared_ptr<arrow::RecordBatch> batch;
  arrow::TableBatchReader reader(*tupleSet.table());
  reader.set_chunksize(tuple::DefaultChunkSize);
  auto res = reader.ReadNext(&batch);
  std::shared_ptr<TupleSet> resultTuples = nullptr;
  while (res.ok() && batch) {

	// Evaluate expressions against a batch
	std::shared_ptr<arrow::ArrayVector> outputs = evaluate(*batch);
	auto batchResultTuples = TupleSet::make(getResultSchema(), *outputs);

	// Concatenate the batch result to the full results
	if (resultTuples)
	  resultTuples = tupleSet.concatenate(batchResultTuples, resultTuples);
	else
	  resultTuples = batchResultTuples;

	res = reader.ReadNext(&batch);
  }

  // if no tuples
  if (!resultTuples) {
    auto outputs = std::make_shared<arrow::ArrayVector>();
    auto outputSchema = getResultSchema();
    // Use StringType for all empty columns
    for (int col_id = 0; col_id < outputSchema->fields().size(); col_id++) {
      auto builder = std::make_shared<::arrow::StringBuilder>();
      std::shared_ptr<arrow::Array> array;
      auto status = builder->Finish(&array);
      if (!status.ok()) {
        throw std::runtime_error(status.message());
      }
      outputs->emplace_back(array);
    }
    resultTuples = TupleSet::make(getResultSchema(), *outputs);
  }

  return resultTuples;

}
