//
// Created by matt on 27/4/20.
//

#include "normal/expression/gandiva/Projector.h"
#include "normal/expression/gandiva/Globals.h"

#include <gandiva/tree_expr_builder.h>
#include <normal/tuple/TupleSet.h>
#include <normal/tuple/Column.h>
#include <normal/tuple/Schema.h>

using namespace normal::expression::gandiva;

Projector::Projector(std::vector<std::shared_ptr<Expression>> Expressions) :
	expressions_(std::move(Expressions)) {}

void Projector::compile(const std::shared_ptr<arrow::Schema> &schema) {

  std::lock_guard<std::mutex> g(BigGlobalLock);

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

arrow::ArrayVector Projector::evaluate(const arrow::RecordBatch &recordBatch) {

#ifndef NDEBUG
  {
	auto res = recordBatch.ValidateFull();
	if (!res.ok()) {
	  throw std::runtime_error(res.message());
	}
  }
#endif

  assert(gandivaProjector_);

  // Evaluate the expressions
  arrow::ArrayVector outputs;
  auto status = gandivaProjector_->Evaluate(recordBatch, arrow::default_memory_pool(), &outputs);

  if (!status.ok()) {
	throw std::runtime_error(status.message());
  }

#ifndef NDEBUG
  {
	for (const auto &array: outputs) {
	  auto res = array->ValidateFull();
	  if (!res.ok()) {
		throw std::runtime_error(res.message());
	  }
	}
  }
#endif

  return outputs;
}

std::shared_ptr<TupleSet> Projector::evaluate(const TupleSet &tupleSet) {

#ifndef NDEBUG
  {
	auto res = tupleSet.table()->ValidateFull();
	if (!res.ok()) {
	  throw std::runtime_error(res.message());
	}
  }
#endif

	std::shared_ptr<TupleSet> resultTupleSet;

  /*
   * Check if the tupleset is valid (has columns) but is empty (0 rows). In this case we want to create
   * and return an empty tupleset
   */
  if (tupleSet.table()->num_columns() > 0 && tupleSet.table()->num_rows() == 0) {
	auto resultSchema = normal::tuple::Schema::make(getResultSchema());
	auto resultColumns = resultSchema->makeColumns();
	auto resultArrays = Column::columnVectorToArrowChunkedArrayVector(resultColumns);
	auto resultTable = ::arrow::Table::Make(getResultSchema(), resultArrays);
	resultTupleSet = TupleSet::make(resultTable);
  } else {
	// Read the table in batches
	std::shared_ptr<arrow::RecordBatch> batch;
	arrow::TableBatchReader reader(*(tupleSet.table()));
//	reader.set_chunksize(tuple::DefaultChunkSize);
	auto res = reader.ReadNext(&batch);

	if (!res.ok()) {
	  throw std::runtime_error(res.message());
	}

	while (batch) {

	  res = batch->ValidateFull();
	  if (!res.ok()) {
		throw std::runtime_error(res.message());
	  }

	  // Evaluate expressions against a batch
	  arrow::ArrayVector outputs = evaluate(*batch);
	  auto batchResultTuples = TupleSet::make(getResultSchema(), outputs);

	  // Concatenate the batch result to the full results
	  if (resultTupleSet)
		resultTupleSet = normal::tuple::TupleSet::concatenate(batchResultTuples, resultTupleSet);
	  else
		resultTupleSet = batchResultTuples;

	  res = reader.ReadNext(&batch);

	  if (!res.ok()) {
		throw std::runtime_error(res.message());
	  }
	}
  }

#ifndef NDEBUG
  {
	auto res = tupleSet.table()->ValidateFull();
	if (!res.ok()) {
	  throw std::runtime_error(res.message());
	}
  }
#endif

  return resultTupleSet;
}
