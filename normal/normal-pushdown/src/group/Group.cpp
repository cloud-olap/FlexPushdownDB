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

  ::arrow::TableBatchReader batchReader(*tupleSet->getArrowTable().value());
  auto batch = batchReader.Next().ValueOrDie();
  while (batch != nullptr) {
	for (int r = 0; r < batch->num_rows(); ++r) {

	  // Build the group key
	  auto groupKey = GroupKey::make();
	  for (const auto &columnName: columnNames_) {
		auto column = batch->GetColumnByName(columnName);
		if (column->type()->id() == ::arrow::Int64Type::type_id) {
		  auto typedColumn = std::static_pointer_cast<::arrow::Int64Array>(column);
		  auto value = ::arrow::MakeScalar(typedColumn->Value(r));
		  auto scalar = std::make_shared<normal::tuple::Scalar>(value);
		  groupKey->append(columnName, scalar);
		} else if (column->type()->id() == ::arrow::StringType::type_id) {
		  /* FIXME: Two problems here:
		   *   1. Directly invoking arrow::MakeScalar gives me "EXC_BAD_ACCESS" exception, don't know why ???
		   *      so instead I cast string to int and make Int32Scalar
		   *   2. Columns from s3 are in default parsed as string type,
		   *      should we add a group column type parameter to specify the type to cast for each group column?
		   */
      auto typedColumn = std::static_pointer_cast<::arrow::StringArray>(column);
		  auto str = typedColumn->GetString(r);
		  auto value = ::arrow::MakeScalar(std::stoi(str));
      auto scalar = std::make_shared<normal::tuple::Scalar>(value);
      groupKey->append(columnName, scalar);
    } else {
		    throw std::runtime_error("Group for column of type '" + column->type()->ToString() + "' not implemented yet");
		  }
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
	  auto arrowTableSlice = arrowTable.value()->Slice(r, 1);
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

	batch = batchReader.Next().ValueOrDie();
  }

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
	for(size_t c = 0;c<columnNames_.size();c++){

	  std::shared_ptr<arrow::Array> array;
	  ::arrow::Int64Builder builder;

	  auto field = schema->getSchema()->field(c);
	  for(const auto &groupAggregateResults: aggregateResults_){
		auto groupKey = groupAggregateResults.first;
		auto groupKeyValue = groupKey->getAttributeValueByName(field->name());
		auto arrowStatus = builder.Append(groupKeyValue->value<long>());
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
