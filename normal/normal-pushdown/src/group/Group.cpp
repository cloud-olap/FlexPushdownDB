//
// Created by matt on 13/5/20.
//

#include <normal/pushdown/group/Group.h>
#include <normal/tuple/TupleSet2.h>

#include <normal/pushdown/group/GroupKey.h>

using namespace normal::pushdown::group;
using namespace normal::tuple;

Group::Group(const std::string &Name,
			 std::vector<std::string> ColumnNames,
			 std::shared_ptr<std::vector<std::shared_ptr<aggregate::AggregationFunction>>> AggregateFunctions) :
	Operator(Name, "Group"),
	columnNames_(std::move(ColumnNames)),
	aggregateFunctions_(std::move(AggregateFunctions)),
	aggregateResults_() {
}

std::shared_ptr<Group> Group::make(const std::string &Name,
								   const std::vector<std::string> &columnNames,
								   const std::shared_ptr<std::vector<std::shared_ptr<aggregate::AggregationFunction>>> &AggregateFunctions) {

  std::vector<std::string> canonicalColumnNames;
  canonicalColumnNames.reserve(columnNames.size());
  for (const auto &columnName: columnNames) {
	canonicalColumnNames.push_back(tuple::ColumnName::canonicalize(columnName));
  }

  return std::make_shared<Group>(Name, canonicalColumnNames, AggregateFunctions);
}

void Group::onReceive(const normal::core::message::Envelope &msg) {
  if (msg.message().type() == "StartMessage") {
	this->onStart();
  } else if (msg.message().type() == "TupleMessage") {
	auto tupleMessage = dynamic_cast<const normal::core::message::TupleMessage &>(msg.message());
	this->onTuple(tupleMessage);
  } else if (msg.message().type() == "CompleteMessage") {
	auto completeMessage = dynamic_cast<const normal::core::message::CompleteMessage &>(msg.message());
	this->onComplete(completeMessage);
  } else {
	// FIXME: Propagate error properly
	throw std::runtime_error("Unrecognized message type " + msg.message().type());
  }
}

void Group::onStart() {
  SPDLOG_DEBUG("Starting");
}

void Group::onTuple(const normal::core::message::TupleMessage &message) {
  auto tupleSet = normal::tuple::TupleSet2::create(message.tuples());

  // Set the input schema if not yet set
  cacheInputSchema(*message.tuples());

  // Fill the groupedTuples
  for (int row_id = 0; row_id < tupleSet->numRows(); row_id++) {

    // Build the group key
    auto groupKey = GroupKey::make();
    for (const auto &columnName: columnNames_) {
      auto &&expectedColumn = tupleSet->getColumnByName(columnName);
      if (!expectedColumn.has_value()) {
        throw(tl::unexpected(expectedColumn.error()));
      }
      auto &&groupColumn = expectedColumn.value();
      auto &&expectedScalar = groupColumn->element(row_id);
      if (!expectedScalar.has_value()) {
        throw(tl::unexpected(expectedScalar.error()));
      }
      auto &&scalar = expectedScalar.value();
      groupKey->append(columnName, scalar);
    }

    // Get or initialise the tuple set for the current group
    std::shared_ptr<normal::tuple::TupleSet2> currentTupleSet;
    auto currentTupleSetIt = groupedTuples_.find(groupKey);
    if (currentTupleSetIt == groupedTuples_.end()) {
      currentTupleSet = normal::tuple::TupleSet2::make (std::make_shared<normal::tuple::Schema>(inputSchema_.value()));
      groupedTuples_.emplace(groupKey, currentTupleSet);
    } else {
      currentTupleSet = currentTupleSetIt->second;
    }

    // Get the current row as a tuple set
    auto arrowTable = tupleSet->getArrowTable();
    auto arrowTableSlice = arrowTable.value()->Slice(row_id, 1);
    auto arrowSliceColumns = arrowTableSlice->columns();
    auto arrowSliceTable = ::arrow::Table::Make(inputSchema_.value(), arrowSliceColumns);
    auto sliceTupleSet = normal::tuple::TupleSet2::make(arrowSliceTable);

    // Append the current row to the current group tuple set
    auto result = currentTupleSet->append(sliceTupleSet);
    if(!result.has_value()){
      // FIXME
      throw std::runtime_error(result.error());
    }
  }

  // Compute aggregate results for each groupTupleSetPair
  for(const auto &groupTupleSetPair: groupedTuples_){

    auto groupKey = groupTupleSetPair.first;
    auto groupTupleSet = groupTupleSetPair.second;

    // Get or initialise the aggregate results for the current group
    std::vector<std::shared_ptr<aggregate::AggregationResult>> currentAggregateResults;
    auto aggregateResultsIt = aggregateResults_.find(groupKey);
    if (aggregateResultsIt == aggregateResults_.end()) {
      for(size_t i=0;i<aggregateFunctions_->size();++i){
      auto aggregateResult = std::make_shared<aggregate::AggregationResult>();
      currentAggregateResults.push_back(aggregateResult);
      }
      aggregateResults_.emplace(groupKey, currentAggregateResults);
    } else {
      currentAggregateResults = aggregateResultsIt->second;
    }

    // Apply the aggregate functions to the current group
    for(size_t i=0;i<aggregateFunctions_->size();i++){
      auto aggregateResult = currentAggregateResults.at(i);
      auto aggregateFunction = aggregateFunctions_->at(i);
      aggregateFunction->apply(aggregateResult, groupTupleSet->toTupleSetV1());
    }
  }
}

void Group::onComplete(const normal::core::message::CompleteMessage&) {

  if (this->ctx()->operatorMap().allComplete(core::OperatorRelationshipType::Producer)) {

    // Finalize the aggregate results
    for(const auto &groupAggregateResults: aggregateResults_) {
      for (size_t i = 0; i < aggregateFunctions_->size(); i++) {
      auto aggregateResult = groupAggregateResults.second.at(i);
      auto aggregateFunction = aggregateFunctions_->at(i);
      aggregateFunction->finalize(aggregateResult);
      }
    }

    // Create output schema
    std::vector<std::shared_ptr<arrow::Field>> fields;
    for(const auto&columnName: columnNames_){
      auto field = inputSchema_.value()->GetFieldByName(columnName);
      fields.emplace_back(field);
    }
    for (const auto &function: *aggregateFunctions_) {
      std::shared_ptr<arrow::Field> field = arrow::field(function->alias(), function->returnType());
      fields.emplace_back(field);
    }
    auto schema = std::make_shared<Schema>(::arrow::schema(fields));

    // Create the group field columns
    std::vector<std::shared_ptr<arrow::Array>> groupFieldColumns;
    for(size_t c = 0; c < columnNames_.size(); c++){

      std::shared_ptr<arrow::Array> array;
      std::shared_ptr<::arrow::ArrayBuilder> builder;

      auto field = schema->getSchema()->field(c);
      if (field->type()->id() == ::arrow::Int32Type::type_id) {
        builder = std::make_shared<::arrow::Int32Builder>();
      } else if (field->type()->id() == ::arrow::Int64Type::type_id) {
        builder = std::make_shared<::arrow::Int64Builder>();
      } else if (field->type()->id() == ::arrow::FloatType::type_id) {
        builder = std::make_shared<::arrow::FloatBuilder>();
      } else if (field->type()->id() == ::arrow::DoubleType::type_id) {
        builder = std::make_shared<::arrow::DoubleBuilder>();
      } else if (field->type()->id() == ::arrow::BooleanType::type_id) {
        builder = std::make_shared<::arrow::BooleanBuilder>();
      } else if (field->type()->id() == ::arrow::StringType::type_id) {
        builder = std::make_shared<::arrow::StringBuilder>();
      } else {
        throw std::runtime_error(
                "Group for column of type '" + field->type()->ToString() + "' not implemented yet");
      }

      for(const auto &groupAggregateResults: aggregateResults_){
        auto groupKey = groupAggregateResults.first;
        auto groupKeyValue = groupKey->getAttributeValueByName(field->name());

        ::arrow::Status arrowStatus;
        if (groupKeyValue->type()->id() == ::arrow::Int32Type::type_id) {
          auto int32Builder = std::static_pointer_cast<::arrow::Int32Builder>(builder);
          arrowStatus = int32Builder->Append(groupKeyValue->value<int32_t>());
        } else if (groupKeyValue->type()->id() == ::arrow::Int64Type::type_id) {
          auto int64Builder = std::static_pointer_cast<::arrow::Int64Builder>(builder);
          arrowStatus = int64Builder->Append(groupKeyValue->value<int64_t>());
        } else if (groupKeyValue->type()->id() == ::arrow::FloatType::type_id) {
          auto floatBuilder = std::static_pointer_cast<::arrow::FloatBuilder>(builder);
          arrowStatus = floatBuilder->Append(groupKeyValue->value<float>());
        } else if (groupKeyValue->type()->id() == ::arrow::DoubleType::type_id) {
          auto doubleBuilder = std::static_pointer_cast<::arrow::DoubleBuilder>(builder);
          arrowStatus = doubleBuilder->Append(groupKeyValue->value<double>());
        } else if (groupKeyValue->type()->id() == ::arrow::BooleanType::type_id) {
          auto booleanBuilder = std::static_pointer_cast<::arrow::BooleanBuilder>(builder);
          arrowStatus = booleanBuilder->Append(groupKeyValue->value<bool>());
        } else if (groupKeyValue->type()->id() == ::arrow::StringType::type_id) {
          auto stringBuilder = std::static_pointer_cast<::arrow::StringBuilder>(builder);
          arrowStatus = stringBuilder->Append(groupKeyValue->value<std::string>());
        }

        if(!arrowStatus.ok()){
          // FIXME
          throw std::runtime_error(arrowStatus.message());
        }
      }

      auto arrowStatus = builder->Finish(&array);
      if(!arrowStatus.ok()){
        // FIXME
        throw std::runtime_error(arrowStatus.message());
      }
      groupFieldColumns.emplace_back(array);
    }

    // Create the aggregate function columns
    std::vector<std::shared_ptr<arrow::Array>> aggregateFunctionColumns;
    for(size_t c = 0;c<aggregateFunctions_->size();++c){

      std::shared_ptr<arrow::Array> array;
      ::arrow::Int64Builder builder;

      auto field = schema->getSchema()->field(c);
      for(const auto &groupAggregateResults: aggregateResults_){
      auto groupResults = groupAggregateResults.second;
      auto groupResultValue = groupResults.at(c);
      auto groupResultValueArrowScalar = groupResultValue->evaluate();
      auto groupResultValueScalar = Scalar::make(groupResultValueArrowScalar);
      auto arrowStatus = builder.Append(groupResultValueScalar->value<long>());
      if(!arrowStatus.ok()){
        // FIXME
        throw std::runtime_error(arrowStatus.message());
      }
      }

      auto arrowStatus = builder.Finish(&array);
      if(!arrowStatus.ok()){
      // FIXME
      throw std::runtime_error(arrowStatus.message());
      }
      aggregateFunctionColumns.emplace_back(array);
    }

    std::vector<std::shared_ptr<::arrow::Array>> columns;
    columns.insert (columns.end(), groupFieldColumns.begin(), groupFieldColumns.end());
    columns.insert (columns.end(), aggregateFunctionColumns.begin(), aggregateFunctionColumns.end());

    auto table = arrow::Table::Make(schema->getSchema(), columns);

    const std::shared_ptr<TupleSet> &groupedTupleSet = TupleSet::make(table);
    auto tupleSetV2 = TupleSet2::create(groupedTupleSet);

    std::shared_ptr<normal::core::message::Message>
      tupleMessage = std::make_shared<normal::core::message::TupleMessage>(groupedTupleSet, this->name());
    ctx()->tell(tupleMessage);

    ctx()->notifyComplete();
  }

}

void Group::cacheInputSchema(const TupleSet &tuples) {
  if(!inputSchema_.has_value()){
	inputSchema_ = tuples.table()->schema();
  }
}
