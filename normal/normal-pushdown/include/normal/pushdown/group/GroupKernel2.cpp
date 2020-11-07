//
// Created by matt on 20/10/20.
//

#include "GroupKernel2.h"
#include "GroupKey2.h"
#include "AggregateBuilderWrapper.h"

#include <utility>

#include <normal/tuple/ArrayAppenderWrapper.h>
#include <normal/tuple/ArrayHasher.h>
#include <iostream>

namespace normal::pushdown::group {

GroupKernel2::GroupKernel2(const std::vector<std::string>& columnNames,
               const std::vector<std::string>& aggregateColumnNames,
						   std::vector<std::shared_ptr<AggregationFunction>> aggregateFunctions) :
	groupColumnNames_(ColumnName::canonicalize(columnNames)),
	aggregateColumnNames_(ColumnName::canonicalize(aggregateColumnNames)),
	aggregateFunctions_(std::move(aggregateFunctions)) {}

tl::expected<void, std::string> GroupKernel2::group(TupleSet2 &tupleSet) {

  // Cache or validate the input schema
  auto expectedCacheResult = cache(tupleSet);
  if (!expectedCacheResult)
	return expectedCacheResult;

  // Check the tuple set is defined
  if (!tupleSet.getArrowTable().has_value())
	return tl::make_unexpected("Tuple set is undefined");
  auto table = tupleSet.getArrowTable().value();

  auto groupedArraysResult = groupTable(*table);
  if (!groupedArraysResult)
	return groupedArraysResult;

  // Compute aggregate results for this tupleSet
  computeGroupAggregates();

  // Clear computed grouped tupleSet
  groupArrayAppenderVectorMap_.clear();
  groupArrayVectorMap_.clear();

  return {};
}

void GroupKernel2::computeGroupAggregates() {

  // Finalise the appenders
  for (const auto &groupArrayAppenders: groupArrayAppenderVectorMap_) {
    std::vector<std::shared_ptr<arrow::Array>> arrays{};
    for (const auto &groupArrayAppender: groupArrayAppenders.second) {
      arrays.push_back(groupArrayAppender->finalize().value());
    }
    groupArrayVectorMap_.emplace(groupArrayAppenders.first, arrays);
  }

  // Compute aggregate results for each groupTupleSetPair
  for (const auto &groupTupleSetPair: groupArrayVectorMap_) {

	auto groupKey = groupTupleSetPair.first;
	auto groupArrays = groupTupleSetPair.second;

	auto groupTupleSet = TupleSet2::make(aggregateSchema_.value(), groupArrays);

	// Get or initialise the aggregate results for the current group
	std::vector<std::shared_ptr<AggregationResult>> currentAggregateResults;
	auto aggregateResultsIt = groupAggregationResultVectorMap_.find(groupKey);
	if (aggregateResultsIt == groupAggregationResultVectorMap_.end()) {
	  for (size_t i = 0; i < aggregateFunctions_.size(); ++i) {
		auto aggregateResult = std::make_shared<AggregationResult>();
		currentAggregateResults.push_back(aggregateResult);
	  }
	  groupAggregationResultVectorMap_.emplace(groupKey, currentAggregateResults);
	} else {
	  currentAggregateResults = aggregateResultsIt->second;
	}

	// Apply the aggregate functions to the current group
	for (size_t i = 0; i < aggregateFunctions_.size(); i++) {
	  auto aggregateResult = currentAggregateResults.at(i);
	  auto aggregateFunction = aggregateFunctions_.at(i);
	  aggregateFunction->apply(aggregateResult, groupTupleSet->toTupleSetV1());
	}
  }
}

tl::expected<void, std::string> GroupKernel2::cache(const TupleSet2 &tupleSet) {

  if(!tupleSet.getArrowTable().has_value())
	return tl::make_unexpected(fmt::format("Input tuple set table is undefined"));
  auto table = tupleSet.getArrowTable().value();

  if (!inputSchema_.has_value()) {

	// Canonicalise and cache the schema
	std::vector<std::shared_ptr<arrow::Field>> fields;
	for(const auto &field : table->schema()->fields()){
	  fields.push_back(field->WithName(ColumnName::canonicalize(field->name())));
	}
	inputSchema_ = arrow::schema(fields);

	// Compute field indices
	for (const auto &columnName: groupColumnNames_) {
	  auto fieldIndex = inputSchema_.value()->GetFieldIndex(columnName);
	  if (fieldIndex == -1)
		return tl::make_unexpected(fmt::format("Group column '{}' not found in input schema", columnName));
	  groupColumnIndices_.push_back(fieldIndex);
	}

	for (const auto &columnName: aggregateColumnNames_) {
    auto fieldIndex = inputSchema_.value()->GetFieldIndex(columnName);
    if (fieldIndex == -1)
    return tl::make_unexpected(fmt::format("Aggregate column '{}' not found in input schema", columnName));
	  aggregateColumnIndices_.push_back(fieldIndex);
	}

	// Create aggregate schema
  std::vector<std::shared_ptr<arrow::Field>> aggregateFields;
  for(const auto &aggregateColumnIndex: aggregateColumnIndices_){
    aggregateFields.push_back(table->schema()->field(aggregateColumnIndex));
  }
  aggregateSchema_ = arrow::schema(aggregateFields);

	return {};
  } else {
	// Check the schema is the same as the cached schema
	if (!inputSchema_.value()->Equals(table->schema())) {
	  return tl::make_unexpected(fmt::format("Input tuple set schema does not match cached input tuple set schema. \n"
											 "schema:\n"
											 "{}\n"
											 "cached schema:\n"
											 "{}",
											 inputSchema_.value()->ToString(),
											 table->schema()->ToString()));
	} else {
	  return {};
	}
  }
}

tl::expected<std::shared_ptr<TupleSet2>, std::string> GroupKernel2::finalise() {

  // Finalize the aggregate results
  for (const auto &groupAggregateResults: groupAggregationResultVectorMap_) {
	for (size_t i = 0; i < aggregateFunctions_.size(); i++) {
	  auto aggregateResult = groupAggregateResults.second.at(i);
	  auto aggregateFunction = aggregateFunctions_.at(i);
	  aggregateFunction->finalize(aggregateResult);
	}
  }

  // Create output array appenders for group columns
  std::vector<std::shared_ptr<ArrayAppender>> groupColumnArrayAppenders;
  groupColumnArrayAppenders.reserve(groupColumnIndices_.size());
  for (const auto &groupColumnIndex: groupColumnIndices_) {
	groupColumnArrayAppenders
		.push_back(ArrayAppenderBuilder::make(inputSchema_.value()->fields()[groupColumnIndex]->type(),
											  groupArrayVectorMap_.size()).value());
  }

  // Create output array builders for aggregate columns
  std::vector<std::shared_ptr<AggregateBuilder>> aggregateBuilders;
  aggregateBuilders.reserve(aggregateFunctions_.size());
  for (const auto &aggregateFunction: aggregateFunctions_) {
  auto returnType = aggregateFunction->returnType();
  if (!returnType) {
    return tl::make_unexpected("No return type from finalise");
  }
	auto expectedAggregateBuilder = makeAggregateBuilder(returnType);
	if(!expectedAggregateBuilder.has_value())
	  return tl::make_unexpected(expectedAggregateBuilder.error());
	aggregateBuilders.push_back(expectedAggregateBuilder.value());
  }

  // Create output schema
  auto expectedOutputSchema = makeOutputSchema();
  if (!expectedOutputSchema)
    return tl::make_unexpected(expectedOutputSchema.error());
  outputSchema_ = expectedOutputSchema.value();

  // Append values
  for (const auto &groupAggregationResultVector: groupAggregationResultVectorMap_) {
    auto groupKey = groupAggregationResultVector.first;
    auto aggregateResultVector = groupAggregationResultVector.second;
    auto groupKeyArrayVector = groupKeyBuffer_.find(groupKey)->second;

    for (size_t c = 0; c < (size_t)outputSchema_.value()->num_fields(); ++c) {
      if (c < groupColumnIndices_.size()) {
        // Add group column
        groupColumnArrayAppenders[c]->appendValue(groupKeyArrayVector[c], 0);
      } else {
        // Add aggregate column data
        int aggregateIndex = c - groupColumnIndices_.size();
        aggregateBuilders[aggregateIndex]->append(aggregateResultVector.at(aggregateIndex));
      }
    }
  }

  // Finalise appenders to output arrays
  arrow::ArrayVector outputArrays;
  outputArrays.reserve(outputSchema_.value()->fields().size());
  for (const auto &groupColumnArrayAppender: groupColumnArrayAppenders) {
	outputArrays.push_back(groupColumnArrayAppender->finalize().value());
  }
  for (const auto &aggregateBuilder: aggregateBuilders) {
	auto expectedOutputArray = aggregateBuilder->finalise();
	if(!expectedOutputArray.has_value())
	  return tl::make_unexpected(expectedOutputArray.error());
	outputArrays.push_back(expectedOutputArray.value());
  }

  return TupleSet2::make(outputSchema_.value(), outputArrays);
}

tl::expected<std::shared_ptr<arrow::Schema>, std::string> GroupKernel2::makeOutputSchema() {

  std::vector<std::shared_ptr<arrow::Field>> fields;
  for (const auto &columnName: groupColumnNames_) {
	auto field = inputSchema_.value()->GetFieldByName(columnName);
	if (!field)
	  return tl::make_unexpected(fmt::format("Group column '{}' not found in input schema", columnName));
	fields.emplace_back(field);
  }

  for (const auto &function: aggregateFunctions_) {
	auto returnType = function->returnType();
	if (!returnType) {
    return tl::make_unexpected("No return type from makeOutputSchema");
	}
	std::shared_ptr<arrow::Field> field = arrow::field(function->alias(), returnType);
	fields.emplace_back(field);
  }

  return arrow::schema(fields);
}

tl::expected<std::vector<std::shared_ptr<ArrayAppender>>, std::string>
makeAppenders(const ::arrow::Schema &schema, const std::vector<int> columnIndices) {
  std::vector<std::shared_ptr<ArrayAppender>> appenders;
  for (auto const &columnIndex: columnIndices) {
    auto field = schema.field(columnIndex);
    auto expectedAppender = ArrayAppenderBuilder::make(field->type(), 0);
    if (!expectedAppender)
      return tl::make_unexpected(expectedAppender.error());
    appenders.push_back(expectedAppender.value());
  }
  return appenders;
}

tl::expected<GroupArrayVectorMap, std::string>
GroupKernel2::groupRecordBatch(const ::arrow::RecordBatch &recordBatch) {

  // For each row, store the group key -> row data in the group map
  for (int r = 0; r < recordBatch.num_rows(); ++r) {

	// Make a group key for the row
	auto expectedGroupKey = GroupKeyBuilder::make(r, groupColumnIndices_, recordBatch);
	if (!expectedGroupKey)
	  return tl::make_unexpected(expectedGroupKey.error());
	auto groupKey = expectedGroupKey.value();

	// Check if we already have an appender vector for the group
	auto maybeAppenderVectorPair = groupArrayAppenderVectorMap_.find(groupKey);

	std::vector<std::shared_ptr<ArrayAppender>> appenderVector;
	if (maybeAppenderVectorPair == groupArrayAppenderVectorMap_.end()) {

	  // New group, create appender vector
	  auto expectedAppenders = makeAppenders(*recordBatch.schema(), aggregateColumnIndices_);
	  if (!expectedAppenders)
		return tl::make_unexpected(expectedAppenders.error());
	  appenderVector = expectedAppenders.value();
	  groupArrayAppenderVectorMap_.emplace(groupKey, appenderVector);

	} else {
	  // Existing group, get appender vector
	  appenderVector = maybeAppenderVectorPair->second;
	}

	// Append row data of aggregate columns for this group
	for (size_t c = 0; c < aggregateColumnIndices_.size(); ++c) {
	  auto aggregateColumnIndex = aggregateColumnIndices_[c];
	  appenderVector[c]->appendValue(recordBatch.column(aggregateColumnIndex), r);
	}

	// Buffer the group key into array
	if (groupKeyBuffer_.find(groupKey) == groupKeyBuffer_.end()) {
    std::vector<std::shared_ptr<arrow::Array>> arrays;
    for (auto const groupColumnIndex: groupColumnIndices_) {
      // Create appender
      auto expectedGroupKeyAppender = ArrayAppenderBuilder::make(recordBatch.schema()->field(groupColumnIndex)->type(),
                                                                 1);
      if (!expectedGroupKeyAppender.has_value())
        return tl::make_unexpected(expectedGroupKeyAppender.error());
      auto groupKeyAppender = expectedGroupKeyAppender.value();
      groupKeyAppender->appendValue(recordBatch.column(groupColumnIndex), r);

      // Finalize into size = 1 array
      arrays.emplace_back(groupKeyAppender->finalize().value());
    }
    groupKeyBuffer_.emplace(groupKey, arrays);
  }
  }

  return {};
}

tl::expected<void, std::string>
GroupKernel2::groupTable(const ::arrow::Table &table) {

  // Create a record batch reader
  arrow::TableBatchReader reader{table};
  reader.set_chunksize(DefaultChunkSize);
  auto recordBatchReadResult = reader.Next();
  if (!recordBatchReadResult.ok())
	return tl::make_unexpected(recordBatchReadResult.status().message());
  auto recordBatch = *recordBatchReadResult;

  while (recordBatch != nullptr) {

	auto expectedGroupArrayVectorMap = groupRecordBatch(*recordBatch);
	if (!expectedGroupArrayVectorMap)
	  return tl::make_unexpected(expectedGroupArrayVectorMap.error());

	// next batch
	recordBatchReadResult = reader.Next();
	if (!recordBatchReadResult.ok())
	  return tl::make_unexpected(recordBatchReadResult.status().message());
	recordBatch = *recordBatchReadResult;
  }

  return {};
}

bool GroupKernel2::hasInput() {
  return !groupAggregationResultVectorMap_.empty();
}

}