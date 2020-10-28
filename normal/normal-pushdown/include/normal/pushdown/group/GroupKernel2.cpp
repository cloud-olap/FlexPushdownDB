//
// Created by matt on 20/10/20.
//

#include "GroupKernel2.h"
#include "GroupKey2.h"

#include <utility>

#include <normal/tuple/ArrayAppenderWrapper.h>
#include <normal/tuple/ArrayHasher.h>
#include <iostream>

namespace normal::pushdown::group {

GroupKernel2::GroupKernel2(std::vector<std::string> columnNames,
						   std::vector<std::shared_ptr<AggregationFunction>> aggregateFunctions) :
	columnNames_(std::move(columnNames)),
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

  return {};
}

void GroupKernel2::computeGroupAggregates() {

  // Compute aggregate results for each groupTupleSetPair
  for (const auto &groupTupleSetPair: groupArrayVectorMap_) {

	auto groupKey = groupTupleSetPair.first;
	auto groupArrays = groupTupleSetPair.second;

	auto groupTupleSet = TupleSet2::make(inputSchema_.value(), groupArrays);

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

  if (!inputSchema_.has_value()) {

	// Cache the schema
	inputSchema_ = tupleSet.getArrowTable().value()->schema();

	// Compute field indices
	for (const auto &columnName: columnNames_) {
	  groupColumnIndices_.push_back(inputSchema_.value()->GetFieldIndex(columnName));
	}

	// Create output schema
	outputSchema_ = makeOutputSchema();

	return {};
  } else {
	// Check the schema is the same as the cached schema
	if (!inputSchema_.value()->Equals(tupleSet.getArrowTable().value()->schema())) {
	  return tl::make_unexpected(fmt::format("Tuple set schema does not match cached tuple set schema. \n"
											 "schema:\n"
											 "{}\n"
											 "cached schema:\n"
											 "{}",
											 inputSchema_.value()->ToString(),
											 tupleSet.getArrowTable().value()->schema()->ToString()));
	} else {
	  return {};
	}
  }
}

std::shared_ptr<TupleSet2> GroupKernel2::finalise() {

  // Finalise the appenders
  for (const auto &groupArrayAppenders: groupArrayAppenderVectorMap_) {
	std::vector<std::shared_ptr<arrow::Array>> arrays{};
	for (const auto &groupArrayAppender: groupArrayAppenders.second) {
	  arrays.push_back(groupArrayAppender->finalize().value());
	}
	groupArrayVectorMap_.emplace(groupArrayAppenders.first, arrays);
  }

  computeGroupAggregates();

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
  std::vector<std::shared_ptr<arrow::ArrayBuilder>> aggregateColumnArrayBuilders;
  aggregateColumnArrayBuilders.reserve(aggregateFunctions_.size());
  for (const auto &aggregateFunction: aggregateFunctions_) {
	aggregateColumnArrayBuilders.push_back(std::make_shared<arrow::DoubleBuilder>());
  }

  for (size_t c = 0; c < (size_t)outputSchema_.value()->num_fields(); ++c) {
	for (const auto &groupArrayVector: groupArrayVectorMap_) {
	  if (c < groupColumnIndices_.size()) {
		// Add group column
		auto groupColumnIndex = groupColumnIndices_[c];
		groupColumnArrayAppenders[c]->appendValue(groupArrayVector.second[groupColumnIndex], 0);
	  } else {
		// Add aggregate column data
		int aggregateIndex = c - groupColumnIndices_.size();
		auto aggregateResult = groupAggregationResultVectorMap_.at(groupArrayVector.first);
		auto aggregateScalar = aggregateResult.at(aggregateIndex)->evaluate();
		std::static_pointer_cast<arrow::DoubleBuilder>(aggregateColumnArrayBuilders[aggregateIndex])
			->Append(Scalar::make(aggregateScalar)->value<double>());
	  }
	}
  }


  // Add appenders and builders to final output arrays
  arrow::ArrayVector outputArrays;
  outputArrays.reserve(outputSchema_.value()->fields().size());
  for (const auto &groupColumnArrayAppender: groupColumnArrayAppenders) {
	outputArrays.push_back(groupColumnArrayAppender->finalize().value());
  }
  for (const auto &aggregateColumnArrayBuilder: aggregateColumnArrayBuilders) {
	std::shared_ptr<arrow::Array> outputArray;
	aggregateColumnArrayBuilder->Finish(&outputArray);
	outputArrays.push_back(outputArray);
  }

  return TupleSet2::make(outputSchema_.value(), outputArrays);
}

std::shared_ptr<arrow::Schema> GroupKernel2::makeOutputSchema() {

  std::vector<std::shared_ptr<arrow::Field>> fields;
  for (const auto &columnName: columnNames_) {
	auto field = inputSchema_.value()->GetFieldByName(columnName);
	fields.emplace_back(field);
  }

  for (const auto &function: aggregateFunctions_) {
	auto returnType = function->returnType();
	if (!returnType) {
	  // FIXME: if no tuple to group by, the return type is missing, here use DoubleType
	  returnType = std::make_shared<arrow::DoubleType>();
	}
	std::shared_ptr<arrow::Field> field = arrow::field(function->alias(), returnType);
	fields.emplace_back(field);
  }

  return arrow::schema(fields);
}

tl::expected<std::vector<std::shared_ptr<ArrayAppender>>, std::string>
makeAppenders(const ::arrow::Schema &schema) {
  std::vector<std::shared_ptr<ArrayAppender>> appenders;
  for (const auto &field: schema.fields()) {
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
	  auto expectedAppenders = makeAppenders(*recordBatch.schema());
	  if (!expectedAppenders)
		return tl::make_unexpected(expectedAppenders.error());
	  appenderVector = expectedAppenders.value();
	  groupArrayAppenderVectorMap_.emplace(groupKey, appenderVector);

	} else {
	  // Existing group, get appender vector
	  appenderVector = maybeAppenderVectorPair->second;
	}

	// Append row data for this group
	for (int c = 0; c < recordBatch.num_columns(); ++c) {
	  appenderVector[c]->appendValue(recordBatch.column(c), r);
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

}