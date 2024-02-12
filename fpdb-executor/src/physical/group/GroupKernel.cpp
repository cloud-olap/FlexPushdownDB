//
// Created by matt on 20/10/20.
//

#include <fpdb/executor/physical/group/GroupKernel.h>
#include <fpdb/plan/prephysical/AggregatePrePFunction.h>
#include <fpdb/tuple/ArrayAppenderWrapper.h>
#include <fpdb/tuple/ColumnBuilder.h>
#include <fpdb/util/Util.h>
#include <utility>

using namespace fpdb::plan::prephysical;
using namespace fpdb::util;

namespace fpdb::executor::physical::group {

GroupKernel::GroupKernel(const vector<string> &groupColumnNames,
                         const vector<shared_ptr<AggregateFunction>> &aggregateFunctions) :
  GroupAbstractKernel(GroupKernelType::GROUP_KERNEL, groupColumnNames, aggregateFunctions) {}

tl::expected<void, string> GroupKernel::group(const std::shared_ptr<TupleSet> &tupleSet) {

  // Cache or validate the input schema
  auto expectedCacheResult = cache(tupleSet);
  if (!expectedCacheResult)
	  return expectedCacheResult;

  // Check the tuple set is defined
  if (!tupleSet->valid())
	  return tl::make_unexpected("Tuple set is undefined");
  auto table = tupleSet->table();

  // Group the tupleSet into appenders
  auto groupedArraysResult = groupTable(*table);
  if (!groupedArraysResult)
	  return groupedArraysResult;

  return {};
}

tl::expected<void, string> GroupKernel::computeGroupAggregates() {

  // Finalise the appenders
  GroupArrayVectorMap groupArrayVectorMap;
  for (const auto &groupArrayAppenders: groupArrayAppenderVectorMap_) {
    vector<shared_ptr<arrow::Array>> arrays{};
    for (const auto &groupArrayAppender: groupArrayAppenders.second) {
      arrays.emplace_back(groupArrayAppender->finalize().value());
    }
    groupArrayVectorMap.emplace(groupArrayAppenders.first, arrays);
  }

  // Compute aggregate results for each groupTupleSetPair
  for (const auto &groupTupleSetPair: groupArrayVectorMap) {
    // Make group tupleSet
    auto groupKey = groupTupleSetPair.first;
    auto groupArrays = groupTupleSetPair.second;
    auto groupTupleSet = TupleSet::make(aggregateInputSchema_.value(), groupArrays);

    // Initialize aggregate result vector
    groupFinalAggregateResultVectorMap_.emplace(groupKey,
                                                vector<shared_ptr<arrow::Scalar>>{aggregateFunctions_.size()});

    // Compute and save aggregate results
    for (uint i = 0; i < aggregateFunctions_.size(); ++i) {
      const auto &function = aggregateFunctions_[i];
      const auto &expAggregateResult = function->computeComplete(groupTupleSet);
      if (!expAggregateResult) {
        return tl::make_unexpected(expAggregateResult.error());
      }
      groupFinalAggregateResultVectorMap_.find(groupKey)->second[i] = *expAggregateResult;
    }
  }

  return {};
}

vector<string> GroupKernel::getAggregateColumnNames() {
  vector<string> aggregateColumnNames;
  for (const auto &aggregateFunction: aggregateFunctions_) {
    aggregateColumnNames = union_(aggregateColumnNames, aggregateFunction->involvedColumnNames());
  }
  return aggregateColumnNames;
}

tl::expected<void, string> GroupKernel::cache(const std::shared_ptr<TupleSet> &tupleSet) {

  if(!tupleSet->valid())
	return tl::make_unexpected(fmt::format("Input tuple set table is undefined"));
  auto table = tupleSet->table();

  if (!inputSchema_.has_value()) {
    // Canonicalize and cache the schema
    vector<shared_ptr<arrow::Field>> fields;
    for(const auto &field : table->schema()->fields()){
      fields.emplace_back(field->WithName(ColumnName::canonicalize(field->name())));
    }
    inputSchema_ = arrow::schema(fields);

    // Compute field indices
    for (const auto &columnName: groupColumnNames_) {
      auto fieldIndex = inputSchema_.value()->GetFieldIndex(columnName);
      if (fieldIndex == -1)
        return tl::make_unexpected(fmt::format("Group column '{}' not found in input schema", columnName));
      groupColumnIndices_.emplace_back(fieldIndex);
    }

    for (const auto &columnName: getAggregateColumnNames()) {
      // Check if it's the case of count(*)
      if (columnName == AggregatePrePFunction::COUNT_STAR_COLUMN) {
        continue;
      }

      auto fieldIndex = inputSchema_.value()->GetFieldIndex(columnName);
      if (fieldIndex == -1)
        return tl::make_unexpected(fmt::format("Aggregate column '{}' not found in input schema", columnName));
      aggregateColumnIndices_.emplace_back(fieldIndex);
    }
    // In case aggregate functions require no input column (e.g., count(*) or aggregate on literals),
    // which can cause aggregateColumnIndices_ to be empty,
    // we need to add at least one column to aggregateColumnIndices_ to make it non-empty.
    if (aggregateColumnIndices_.empty() && !aggregateFunctions_.empty()) {
      aggregateColumnIndices_.emplace_back(0);
    }

    // Create aggregate input schema
    vector<shared_ptr<arrow::Field>> aggregateFields;
    for(const auto &aggregateColumnIndex: aggregateColumnIndices_){
      aggregateFields.emplace_back(table->schema()->field(aggregateColumnIndex));
    }
    aggregateInputSchema_ = arrow::schema(aggregateFields);

    return {};
  }

  else {
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

tl::expected<shared_ptr<TupleSet>, string> GroupKernel::finalise() {
  // If has no result, return an empty tupleSet
  if (!hasResult()) {
    return finaliseEmpty();
  }

  // Compute aggregate results
  auto computeResult = computeGroupAggregates();
  if (!computeResult) {
    return tl::make_unexpected(computeResult.error());
  }

  // Create column builders for group columns
  vector<shared_ptr<ColumnBuilder>> groupColumnBuilders;
  for (const auto &groupColumnIndex: groupColumnIndices_) {
    const auto &groupField = inputSchema_.value()->field(groupColumnIndex);
    groupColumnBuilders.emplace_back(ColumnBuilder::make(groupField->name(), groupField->type()));
  }

  // Create column builders for aggregate columns
  vector<shared_ptr<ColumnBuilder>> aggregateColumnBuilders;
  for (const auto &function: aggregateFunctions_) {
    aggregateColumnBuilders.emplace_back(ColumnBuilder::make(function->getOutputColumnName(),
                                                             function->returnType()));
  }

  // Append values
  for (const auto &groupAggregateResultIt: groupFinalAggregateResultVectorMap_) {
    const auto &groupKey = groupAggregateResultIt.first;
    const auto &expGroupKeyScalars = groupKey->getScalars();
    if (!expGroupKeyScalars.has_value()) {
      return tl::make_unexpected(expGroupKeyScalars.error());
    }
    const auto &groupKeyScalars = expGroupKeyScalars.value();
    const auto &aggregateResults = groupAggregateResultIt.second;

    // For group column
    for (uint c = 0; c < groupKeyScalars.size(); ++c) {
      groupColumnBuilders[c]->append(Scalar::make(groupKeyScalars[c]));
    }

    // For aggregate column
    for (uint c = 0; c < aggregateFunctions_.size(); ++c) {
      const auto &aggregateResult = aggregateResults[c];
      aggregateColumnBuilders[c]->append(Scalar::make(aggregateResult));
    }
  }

  // Finalize columnBuilders to columns
  vector<shared_ptr<Column>> outputColumns;
  outputColumns.reserve(groupColumnBuilders.size() + aggregateColumnBuilders.size());
  for (const auto &columnBuilder: groupColumnBuilders) {
    const auto expColumn = columnBuilder->finalize();
    if (!expColumn.has_value()) {
      return tl::make_unexpected(expColumn.error());
    }
    outputColumns.emplace_back(expColumn.value());
  }
  for (const auto &columnBuilder: aggregateColumnBuilders) {
    const auto expColumn = columnBuilder->finalize();
    if (!expColumn.has_value()) {
      return tl::make_unexpected(expColumn.error());
    }
    outputColumns.emplace_back(expColumn.value());
  }

  return TupleSet::make(outputColumns);
}

tl::expected<shared_ptr<TupleSet>, string> GroupKernel::finaliseEmpty() {
  if (!inputSchema_.has_value()) {
    return tl::make_unexpected("InputSchema not set");
  }
  if (!aggregateInputSchema_.has_value()) {
    return tl::make_unexpected("AggregateInputSchema not set");
  }

  // Obtain returnType of aggregate functions
  for (const auto &function: aggregateFunctions_) {
    auto compileResult = function->compile(aggregateInputSchema_.value());
    if (!compileResult.has_value()) {
      return tl::make_unexpected(compileResult.error());
    }
  }

  // Make output schema
  arrow::FieldVector fields;
  for (const auto &groupColumnIndex: groupColumnIndices_) {
    fields.emplace_back(inputSchema_.value()->field(groupColumnIndex));
  }
  for (const auto &function: aggregateFunctions_) {
    fields.emplace_back(make_shared<arrow::Field>(function->getOutputColumnName(), function->returnType()));
  }
  auto outputSchema = arrow::schema(fields);

  return TupleSet::make(outputSchema);
}

tl::expected<vector<shared_ptr<ArrayAppender>>, string>
makeAppenders(const ::arrow::Schema &schema, const vector<int>& columnIndices) {
  vector<shared_ptr<ArrayAppender>> appenders;
  for (auto const &columnIndex: columnIndices) {
    const auto& field = schema.field(columnIndex);
    auto expectedAppender = ArrayAppenderBuilder::make(field->type(), 0);
    if (!expectedAppender)
      return tl::make_unexpected(expectedAppender.error());
    appenders.emplace_back(expectedAppender.value());
  }
  return appenders;
}

tl::expected<GroupArrayVectorMap, string>
GroupKernel::groupRecordBatch(const ::arrow::RecordBatch &recordBatch) {

  // For each row, store the group key -> row data in the group map
  for (int64_t r = 0; r < recordBatch.num_rows(); ++r) {

    // Make a group key for the row
    auto expectedGroupKey = TupleKeyBuilder::make(r, groupColumnIndices_, recordBatch);
    if (!expectedGroupKey)
      return tl::make_unexpected(expectedGroupKey.error());
    auto groupKey = expectedGroupKey.value();

    // Check if we already have an appender vector for the group
    auto maybeAppenderVectorPair = groupArrayAppenderVectorMap_.find(groupKey);

    vector<shared_ptr<ArrayAppender>> appenderVector;
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
  }

  return {};
}

tl::expected<void, string> GroupKernel::groupTable(const arrow::Table &table) {

  // Create a record batch reader
  arrow::TableBatchReader reader{table};
  reader.set_chunksize((int64_t) DefaultChunkSize);
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

bool GroupKernel::hasResult() {
  return !groupArrayAppenderVectorMap_.empty();
}

void GroupKernel::clear() {
  groupArrayAppenderVectorMap_.clear();
  groupFinalAggregateResultVectorMap_.clear();
}

}