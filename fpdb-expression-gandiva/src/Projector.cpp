//
// Created by matt on 27/4/20.
//

#include "fpdb/expression/gandiva/Projector.h"
#include "fpdb/expression/gandiva/Globals.h"

#include <gandiva/tree_expr_builder.h>
#include <fpdb/tuple/TupleSet.h>
#include <fpdb/tuple/Column.h>
#include <fpdb/tuple/Schema.h>

using namespace fpdb::expression::gandiva;

Projector::Projector(std::vector<std::shared_ptr<Expression>> Expressions) :
	expressions_(std::move(Expressions)) {}

tl::expected<void, std::string> Projector::compile(const std::shared_ptr<arrow::Schema> &schema) {

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
    return tl::make_unexpected(status.message());
  }

  // Prepare the schema for the results
  auto resultFields = std::vector<std::shared_ptr<arrow::Field>>();
  for (const auto &e: expressions_) {
	  resultFields.emplace_back(arrow::field(e->alias(), e->getReturnType()));
  }
  resultSchema_ = arrow::schema(resultFields);

  return {};
}

tl::expected<arrow::ArrayVector, std::string> Projector::evaluate(const arrow::RecordBatch &recordBatch) {

#ifndef NDEBUG
  {
    auto res = recordBatch.ValidateFull();
    if (!res.ok()) {
      return tl::make_unexpected(res.message());
    }
  }
#endif

  assert(gandivaProjector_);

  // Evaluate the expressions
  arrow::ArrayVector outputs;
  auto status = gandivaProjector_->Evaluate(recordBatch, arrow::default_memory_pool(), &outputs);

  if (!status.ok()) {
    return tl::make_unexpected(status.message());
  }

#ifndef NDEBUG
  {
    for (const auto &array: outputs) {
      auto res = array->ValidateFull();
      if (!res.ok()) {
        return tl::make_unexpected(res.message());
      }
    }
  }
#endif

  return outputs;
}

tl::expected<std::shared_ptr<TupleSet>, std::string> Projector::evaluate(const TupleSet &tupleSet) {

#ifndef NDEBUG
  {
    auto res = tupleSet.table()->ValidateFull();
    if (!res.ok()) {
      return tl::make_unexpected(res.message());
    }
  }
#endif

	std::shared_ptr<TupleSet> resultTupleSet;

  /*
   * Check if the tupleset is valid (has columns) but is empty (0 rows). In this case we want to create
   * and return an empty tupleset
   */
  if (tupleSet.table()->num_columns() > 0 && tupleSet.table()->num_rows() == 0) {
    auto resultSchema = fpdb::tuple::Schema::make(getResultSchema());
    auto resultColumns = resultSchema->makeColumns();
    auto resultArrays = Column::columnVectorToArrowChunkedArrayVector(resultColumns);
    auto resultTable = ::arrow::Table::Make(getResultSchema(), resultArrays);
    resultTupleSet = TupleSet::make(resultTable);
  }

  else {
    // Read the table in batches
    std::shared_ptr<arrow::RecordBatch> batch;
    arrow::TableBatchReader reader(*(tupleSet.table()));
  //	reader.set_chunksize(tuple::DefaultChunkSize);
    auto res = reader.ReadNext(&batch);

    if (!res.ok()) {
      return tl::make_unexpected(res.message());
    }

    while (batch) {

      res = batch->ValidateFull();
      if (!res.ok()) {
        return tl::make_unexpected(res.message());
      }

      // Evaluate expressions against a batch
      auto expOutputs = evaluate(*batch);
      if (!expOutputs.has_value()) {
        return tl::make_unexpected(expOutputs.error());
      }

      auto batchResultTuples = TupleSet::make(getResultSchema(), *expOutputs);

      // Concatenate the batch result to the full results
      if (resultTupleSet) {
        const auto &appendResult = resultTupleSet->append(batchResultTuples);
        if (!appendResult.has_value()) {
          return tl::make_unexpected(appendResult.error());
        }
      }
      else
        resultTupleSet = batchResultTuples;

      res = reader.ReadNext(&batch);

      if (!res.ok()) {
        return tl::make_unexpected(res.message());
      }
    }
  }

#ifndef NDEBUG
  {
    auto res = tupleSet.table()->ValidateFull();
    if (!res.ok()) {
      return tl::make_unexpected(res.message());
    }
  }
#endif

  return resultTupleSet;
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
