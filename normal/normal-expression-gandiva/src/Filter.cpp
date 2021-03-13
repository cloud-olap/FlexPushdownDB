//
// Created by matt on 6/5/20.
//

#include <gandiva/tree_expr_builder.h>

#include <utility>
#include <normal/tuple/Globals.h>

#include "normal/expression/gandiva/Filter.h"
#include "normal/expression/gandiva/Globals.h"

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
  // Maximum chunk size Gandiva filter evaluates at a time
	reader.set_chunksize(normal::tuple::DefaultChunkSize);
	arrowStatus = reader.ReadNext(&batch);
	if (!arrowStatus.ok()) {
	  throw std::runtime_error(arrowStatus.message());
	}

	while (batch != nullptr) {

	  assert(batch->ValidateFull().ok());
	  std::shared_ptr<::gandiva::SelectionVector> selection_vector;
	  auto status = ::gandiva::SelectionVector::MakeInt32(batch->num_rows(), ::arrow::default_memory_pool(), &selection_vector);
	  if (!status.ok()) {
		throw std::runtime_error(status.message());
	  }

	  status = gandivaFilter_->Evaluate(*batch, selection_vector);

	  if (!status.ok()) {
		throw std::runtime_error(status.message());
	  }

//	  SPDLOG_DEBUG("Evaluated SelectionVector  |  vector: {}", selection_vector->ToArray()->ToString());

	  // Evaluate the expressions
	  std::shared_ptr<::arrow::Table> batchArrowTable;

	  /**
	   * NOTE: Gandiva fails if the projector is evaluated using an empty selection vector, so need to test for it
	   */
	  if(selection_vector->GetNumSlots() > 0) {
		arrow::ArrayVector outputs;
		status = gandivaProjector_->Evaluate(*batch, selection_vector.get(), arrow::default_memory_pool(), &outputs);

		if (!status.ok()) {
		  throw std::runtime_error(status.message());
		}

		batchArrowTable = ::arrow::Table::Make(batch->schema(), outputs);
	  }
	  else{
	    auto columns = Schema::make(batch->schema())->makeColumns();
	    auto arrowArrays = Column::columnVectorToArrowChunkedArrayVector(columns);
		batchArrowTable = ::arrow::Table::Make(batch->schema(), arrowArrays);
	  }

	  auto batchTupleSet = std::make_shared<normal::tuple::TupleSet2>(batchArrowTable);

//	  SPDLOG_DEBUG("Filtered batch:\n{}",
//				   batchTupleSet->showString(normal::tuple::TupleSetShowOptions(normal::tuple::TupleSetShowOrientation::RowOriented)));

	  auto result = filteredTupleSet->append(batchTupleSet);
	  if(!result.has_value()){
		throw std::runtime_error(result.error());
	  }

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

void Filter::compile(const std::shared_ptr<normal::tuple::Schema> &schema) {
  // Use lock the compilation phase as it is not necessarily thread safe
  std::lock_guard<std::mutex> g(BigGlobalLock);
  // Compile the expressions
  pred_->compile(schema->getSchema());

  // build ::gandiva::Filter
  auto gandivaCondition = ::gandiva::TreeExprBuilder::MakeCondition(pred_->getGandivaExpression());
  gandivaFilter_ = buildGandivaFilter(gandivaCondition, schema);

  // build ::gandiva::Projector
  gandivaProjector_ = buildGandivaProjector(schema);
}

std::shared_ptr<::gandiva::Filter> normal::expression::gandiva::buildGandivaFilter(
        const std::shared_ptr<::gandiva::Condition> &gandivaCondition,
        const std::shared_ptr<normal::tuple::Schema> &schema) {
  auto gandivaConditionStr = gandivaCondition->ToString();
  auto schemaStr = schema->getSchema()->ToString();
  auto key = gandivaConditionStr + ", " + schemaStr;
  auto gandivaFilterPair = gandivaFilterMap_.find(key);

  if (gandivaFilterPair != gandivaFilterMap_.end()) {
    return gandivaFilterPair->second;
  } else {
//    SPDLOG_INFO("Build gandiva filter: {}", gandivaConditionStr);
    // Build a filter for the predicate.
    std::shared_ptr<::gandiva::Filter> gandivaFilter;
    auto status = ::gandiva::Filter::Make(schema->getSchema(),
                                          gandivaCondition,
                                          ::gandiva::ConfigurationBuilder::DefaultConfiguration(),
                                          &gandivaFilter);
    if (!status.ok()) {
      throw std::runtime_error(status.message());
    }
    gandivaFilterMap_.emplace(key, gandivaFilter);
    return gandivaFilter;
  }
}

std::shared_ptr<::gandiva::Projector> normal::expression::gandiva::buildGandivaProjector(
        const std::shared_ptr<normal::tuple::Schema>& schema) {
  auto schemaStr = schema->getSchema()->ToString();
  const auto& key = schemaStr;
  auto gandivaProjectorPair = gandivaProjectorMap_.find(key);

  if (gandivaProjectorPair != gandivaProjectorMap_.end()) {
    return gandivaProjectorPair->second;
  } else {
//    SPDLOG_INFO("Build gandiva projector: {}", schemaStr);
    // Create a pass through expression
    std::vector<std::shared_ptr<::gandiva::Expression>> fieldExpressions;
    for (const auto &field: schema->fields()) {
      auto gandivaField = ::gandiva::TreeExprBuilder::MakeField(field);
      auto fieldExpression = ::gandiva::TreeExprBuilder::MakeExpression(gandivaField, field);
      fieldExpressions.push_back(fieldExpression);
    }

    // Build a projector for the pass through expression
    std::shared_ptr<::gandiva::Projector> gandivaProjector;
    auto status = ::gandiva::Projector::Make(schema->getSchema(),
                                        fieldExpressions,
                                        ::gandiva::SelectionVector::MODE_UINT32,
                                        ::gandiva::ConfigurationBuilder::DefaultConfiguration(),
                                        &gandivaProjector);

    if (!status.ok()) {
      throw std::runtime_error(status.message());
    }
    gandivaProjectorMap_.emplace(key, gandivaProjector);
    return gandivaProjector;
  }
}

void normal::expression::gandiva::clearGandivaProjectorAndFilter() {
  gandivaFilterMap_.clear();
  gandivaProjectorMap_.clear();
}
