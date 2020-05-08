//
// Created by matt on 6/5/20.
//

#include <gandiva/tree_expr_builder.h>

#include <utility>
#include <normal/tuple/Globals.h>

#include "normal/expression/gandiva/Filter.h"

using namespace normal::expression::gandiva;

Filter::Filter(std::shared_ptr<Expression> Pred) : pred_(std::move(Pred)) {}

std::shared_ptr<Filter> Filter::make(const std::shared_ptr<Expression> &Pred) {
  return std::make_shared<Filter>(Pred);
}

std::shared_ptr<normal::tuple::TupleSet2> Filter::evaluate(const normal::tuple::TupleSet2 &tupleSet) {

  if (tupleSet.schema().has_value()) {

	auto filteredTupleSet = normal::tuple::TupleSet2::make(tupleSet.schema().value());
	auto arrowTable = tupleSet.getArrowTable().value();

	assert(arrowTable->ValidateFull().ok());

	arrow::Status arrowStatus;

	std::shared_ptr<arrow::RecordBatch> batch;
	arrow::TableBatchReader reader(*arrowTable);
	reader.set_chunksize(normal::tuple::DefaultChunkSize);
	arrowStatus = reader.ReadNext(&batch);
	if (!arrowStatus.ok()) {
	  throw std::runtime_error(arrowStatus.message());
	}

	assert(batch->ValidateFull().ok());

	while (batch != nullptr) {

	  std::shared_ptr<::gandiva::SelectionVector> selection_vector;
	  auto status = ::gandiva::SelectionVector::MakeInt16(batch->num_rows(), ::arrow::default_memory_pool(), &selection_vector);

	  if (!status.ok()) {
		throw std::runtime_error(status.message());
	  }

	  status = gandivaFilter_->Evaluate(*batch, selection_vector);

	  if (!status.ok()) {
		throw std::runtime_error(status.message());
	  }

	  // Evaluate the expressions
	  arrow::ArrayVector outputs;
	  status = gandivaProjector_->Evaluate(*batch, selection_vector.get(), arrow::default_memory_pool(), &outputs);

	  if (!status.ok()) {
		throw std::runtime_error(status.message());
	  }

	  auto batchArrowTable = ::arrow::Table::Make(batch->schema(), outputs);
	  auto batchTupleSet = std::make_shared<normal::tuple::TupleSet2>(batchArrowTable);

	  SPDLOG_DEBUG("Filtered batch:\n{}",
				   batchTupleSet->showString(normal::tuple::TupleSetShowOptions(normal::tuple::TupleSetShowOrientation::RowOriented)));

	  filteredTupleSet->append(batchTupleSet);

	  arrowStatus = reader.ReadNext(&batch);
	  if (!arrowStatus.ok()) {
		throw std::runtime_error(arrowStatus.message());
	  }
	}

	return filteredTupleSet;
  } else {
	// FIXME: Does it make sense to have a tuple set with no schema? Maybe it should be forbidden. DB's tend to register
	//  schema less tables as defined but not queryable. I.e. the "table" exists but not the "relation".
	//  Raise an error for now
	throw std::runtime_error("Cannot filter tuple set. Tuple set schema is undefined.");
  }
}

void Filter::compile(const std::shared_ptr<normal::tuple::Schema> &Schema) {

  // Compile the expressions
  pred_->compile(Schema->getSchema());

  gandivaCondition_ = ::gandiva::TreeExprBuilder::MakeCondition(pred_->getGandivaExpression());

  SPDLOG_DEBUG("Filter predicate:\n{}", gandivaCondition_->ToString());

  // Build a filter for the predicate.
  auto status = ::gandiva::Filter::Make(Schema->getSchema(),
										gandivaCondition_,
										::gandiva::ConfigurationBuilder::DefaultConfiguration(),
										&gandivaFilter_);
  if (!status.ok()) {
	throw std::runtime_error(status.message());
  }

  // Create a pass through expression
  std::vector<std::shared_ptr<::gandiva::Expression>> fieldExpressions;
  for (const auto &field: Schema->fields()) {
	auto gandivaField = ::gandiva::TreeExprBuilder::MakeField(field);
	auto fieldExpression = ::gandiva::TreeExprBuilder::MakeExpression(gandivaField, field);
	fieldExpressions.push_back(fieldExpression);
  }

  // Build a projector for the pass through expression
  status = ::gandiva::Projector::Make(Schema->getSchema(),
									  fieldExpressions,
									  ::gandiva::SelectionVector::MODE_UINT16,
									  ::gandiva::ConfigurationBuilder::DefaultConfiguration(),
									  &gandivaProjector_);

  if (!status.ok()) {
	throw std::runtime_error(status.message());
  }
}
