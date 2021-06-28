//
// Created by matt on 27/4/20.
//

#include "normal/expression/simple/Projector.h"

using namespace normal::expression::simple;

Projector::Projector(std::vector<std::shared_ptr<Expression>> exprs) :
	expressions_(std::move(exprs)) {}

std::shared_ptr<arrow::Schema> Projector::getResultSchema() {
  // Prepare the schema for the results
  auto resultFields = std::vector<std::shared_ptr<arrow::Field>>();
  for (const auto &e: expressions_) {
	resultFields.emplace_back(arrow::field(e->alias(), e->getReturnType()));
  }
  return arrow::schema(resultFields);
}

arrow::ArrayVector Projector::evaluate(const arrow::RecordBatch &batch) {

  // Evaluate the expressions
  arrow::ArrayVector outputs;

  for (const auto &expression: expressions_) {
	outputs.emplace_back(expression->evaluate(batch).value());
  }

  return outputs;
}

std::shared_ptr<TupleSet> Projector::evaluate(const TupleSet &tupleSet) {

  // Read the table in batches
  std::shared_ptr<arrow::RecordBatch> batch;
  arrow::TableBatchReader reader(*tupleSet.table());
//  reader.set_chunksize(tuple::DefaultChunkSize);
  auto res = reader.ReadNext(&batch);
  std::shared_ptr<TupleSet> resultTuples = nullptr;
  while (res.ok() && batch) {

	// Evaluate expressions against a batch
	arrow::ArrayVector outputs = evaluate(*batch);
	auto batchResultTuples = TupleSet::make(getResultSchema(), outputs);

	// Concatenate the batch result to the full results
	if (resultTuples)
	  resultTuples = TupleSet::concatenate(batchResultTuples, resultTuples);
	else
	  resultTuples = batchResultTuples;

	res = reader.ReadNext(&batch);
  }

  return resultTuples;

}

void Projector::compile(const std::shared_ptr<arrow::Schema> &) {
  // NOOP
}
