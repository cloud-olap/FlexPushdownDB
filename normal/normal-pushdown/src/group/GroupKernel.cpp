//
// Created by matt on 20/10/20.
//

#include "normal/pushdown/group/GroupKernel.h"

#include <utility>

#include <normal/tuple/ArrayAppenderWrapper.h>

namespace normal::pushdown::group {

GroupKernel::GroupKernel(std::vector<std::string> columnNames,
						 std::vector<std::shared_ptr<AggregationFunction>> aggregateFunctions) :
	columnNames_(std::move(columnNames)),
	aggregateFunctions_(std::move(aggregateFunctions)) {}

void GroupKernel::onTuple(const TupleSet2 &tupleSet) {

//  // empty tuple
//  if (tupleSet->numRows() == 0) {
//    return;
//  }

  // Set the input schema if not yet set
  cacheInputSchema(tupleSet);

  auto startTime = std::chrono::steady_clock::now();
  // Fill the groupedTuples using record batch
  std::shared_ptr<::arrow::Table> table;
  if (tupleSet.getArrowTable().has_value()) {
	table = tupleSet.getArrowTable().value();
  } else {
	throw std::runtime_error("TupleSet is undefined");
  }
  auto arrowSchema = table->schema();

  arrow::TableBatchReader reader{*table};
  reader.set_chunksize(DefaultChunkSize);
  arrow::Result<std::shared_ptr<::arrow::RecordBatch>> recordBatchResult = reader.Next();
  if (!recordBatchResult.ok()) {
	throw std::runtime_error(recordBatchResult.status().message());
  }
  auto recordBatch = *recordBatchResult;

  /**
   * compute the size of batch
   */
  size_t size = 0;
  if (recordBatch) {
	for (int col_id = 0; col_id < recordBatch->num_columns(); col_id++) {
	  auto array = recordBatch->column(col_id);
	  for (auto const &buffer: array->data()->buffers) {
		size += buffer->size();
	  }
	}
  }
  size_t restRowNum = tupleSet.numRows();
  /**
   * end
   */

  // temporary grouped arrays
//  std::unordered_map<std::shared_ptr<GroupKey>, std::shared_ptr<std::vector<std::shared_ptr<ArrayAppender>>>, GroupKeyPointerHash, GroupKeyPointerPredicate> tempGroupedArrays;
  std::unordered_map<std::shared_ptr<GroupKey>,
					 std::vector<std::vector<std::shared_ptr<arrow::Array>>>,
					 GroupKeyPointerHash,
					 GroupKeyPointerPredicate> groupedArrays;

  while (recordBatch) {
	auto start = std::chrono::steady_clock::now();
	// FIXME: directly using arrow::Table to get scalar leads to a bug, "GetString(i)", so here make a tupleSet first
	std::vector<std::shared_ptr<arrow::Array>> batchArrays;
	for (int col_id = 0; col_id < recordBatch->num_columns(); ++col_id) {
	  batchArrays.emplace_back(recordBatch->column(col_id));
	}
	auto batchTupleSet = TupleSet2::make(arrowSchema, batchArrays);

	// initialize appenders
	std::unordered_map<std::shared_ptr<GroupKey>,
					   std::shared_ptr<std::vector<std::shared_ptr<ArrayAppender>>>,
					   GroupKeyPointerHash,
					   GroupKeyPointerPredicate> tempGroupedAppenders;

	// process for each tuple
	for (int row_id = 0; row_id < batchTupleSet->numRows(); ++row_id) {
	  // build the group key
	  auto groupKey = GroupKey::make();
	  for (const auto &columnName: columnNames_) {
		auto &&expectedColumn = batchTupleSet->getColumnByName(columnName);
		if (!expectedColumn.has_value()) {
		  throw (tl::unexpected(expectedColumn.error()));
		}
		auto &&groupColumn = expectedColumn.value();
		auto &&expectedScalar = groupColumn->element(row_id);
		if (!expectedScalar.has_value()) {
		  throw (tl::unexpected(expectedScalar.error()));
		}
		auto &&scalar = expectedScalar.value();
		groupKey->append(columnName, scalar);
	  }

	  std::shared_ptr<std::vector<std::shared_ptr<ArrayAppender>>> appenders;
	  auto currentAppendersIt = tempGroupedAppenders.find(groupKey);
	  if (currentAppendersIt == tempGroupedAppenders.end()) {
		appenders = std::make_shared<std::vector<std::shared_ptr<ArrayAppender>>>();
		for (int col_id = 0; col_id < recordBatch->num_columns(); ++col_id) {
		  auto expectedAppender = ArrayAppenderBuilder::make(arrowSchema->field(col_id)->type(), tupleSet.numRows());
		  if (!expectedAppender.has_value()) {
			throw std::runtime_error(expectedAppender.error());
		  }
		  appenders->emplace_back(expectedAppender.value());
		}
		tempGroupedAppenders.emplace(groupKey, appenders);
	  } else {
		appenders = currentAppendersIt->second;
	  }

	  // append values of the current tuple
	  for (int col_id = 0; col_id < recordBatch->num_columns(); ++col_id) {
		appenders->at(col_id)->appendValue(recordBatch->column(col_id), row_id);
	  }
	}
	auto end = std::chrono::steady_clock::now();
//    SPDLOG_INFO("Append time: {}, size: {}, name: {}",
//                std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count(),
//                recordBatch->num_rows(),
//                name());
	start = end;

	// finalize to arrays for the current batch
	for (auto const &columnAppendersIt: tempGroupedAppenders) {
	  auto groupKey = columnAppendersIt.first;
	  auto appenders = columnAppendersIt.second;
	  for (int col_id = 0; col_id < arrowSchema->num_fields(); ++col_id) {
		auto expectedArray = appenders->at(col_id)->finalize();
		if (!expectedArray.has_value()) {
		  throw std::runtime_error(expectedArray.error());
		}
		auto currentGroupedArraysIt = groupedArrays.find(groupKey);
		if (currentGroupedArraysIt == groupedArrays.end()) {
		  std::vector<std::vector<std::shared_ptr<arrow::Array>>>
			  columnArrays{static_cast<size_t>(arrowSchema->num_fields())};
		  columnArrays[col_id].emplace_back(expectedArray.value());
		  groupedArrays.emplace(groupKey, columnArrays);
		} else {
		  currentGroupedArraysIt->second[col_id].emplace_back(expectedArray.value());
		}
	  }
	}
	end = std::chrono::steady_clock::now();
//    SPDLOG_INFO("Finalize time: {}, size: {}, name: {}",
//                std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count(),
//                recordBatch->num_rows(),
//                name());

	restRowNum -= recordBatch->num_rows();
//    SPDLOG_INFO("Grouped {} tuples, rest: {} bytesize: {}, name: {}", recordBatch->num_rows(), restRowNum, size, name());

	// next batch
	recordBatchResult = reader.Next();
	if (!recordBatchResult.ok()) {
	  throw std::runtime_error(recordBatchResult.status().message());
	}
	recordBatch = *recordBatchResult;
  }

  for (auto const &columnArraysIt: groupedArrays) {
	auto groupKey = columnArraysIt.first;
	auto columnArrays = columnArraysIt.second;
	std::vector<std::shared_ptr<::arrow::ChunkedArray>> chunkedArrays;
	for (const auto &arrays : columnArrays) {
	  auto chunkedArray = std::make_shared<::arrow::ChunkedArray>(arrays);
	  chunkedArrays.emplace_back(chunkedArray);
	}
	std::shared_ptr<::arrow::Table> table = ::arrow::Table::Make(tupleSet.schema().value()->getSchema(), chunkedArrays);
	auto tupleSet = TupleSet2::make(table);
	groupedTuples_.emplace(groupKey, tupleSet);
  }

  auto endTime = std::chrono::steady_clock::now();
//  SPDLOG_INFO("Group key time: {}, size: {}, name: {}",
//              std::chrono::duration_cast<std::chrono::nanoseconds>(endTime - startTime).count(),
//              tupleSet->numRows(),
//              name());
  startTime = endTime;

  // Compute aggregate results for each groupTupleSetPair
  for (const auto &groupTupleSetPair: groupedTuples_) {

	auto groupKey = groupTupleSetPair.first;
	auto groupTupleSet = groupTupleSetPair.second;

	// Get or initialise the aggregate results for the current group
	std::vector<std::shared_ptr<aggregate::AggregationResult>> currentAggregateResults;
	auto aggregateResultsIt = aggregateResults_.find(groupKey);
	if (aggregateResultsIt == aggregateResults_.end()) {
	  for (size_t i = 0; i < aggregateFunctions_.size(); ++i) {
		auto aggregateResult = std::make_shared<aggregate::AggregationResult>();
		currentAggregateResults.push_back(aggregateResult);
	  }
	  aggregateResults_.emplace(groupKey, currentAggregateResults);
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

  endTime = std::chrono::steady_clock::now();
//  SPDLOG_INFO("Aggregate time: {}, size: {}, name: {}",
//              std::chrono::duration_cast<std::chrono::nanoseconds>(endTime - startTime).count(),
//              tupleSet->numRows(),
//              name());

  // clear computed groupedTuples
  groupedTuples_.clear();
}

void GroupKernel::cacheInputSchema(const TupleSet2 &tupleSet) {
  if (!inputSchema_.has_value()) {
	inputSchema_ = tupleSet.getArrowTable().value()->schema();
  }
}

std::shared_ptr<TupleSet2> GroupKernel::group() {
  // Finalize the aggregate results
  for (const auto &groupAggregateResults: aggregateResults_) {
	for (size_t i = 0; i < aggregateFunctions_.size(); i++) {
	  auto aggregateResult = groupAggregateResults.second.at(i);
	  auto aggregateFunction = aggregateFunctions_.at(i);
	  aggregateFunction->finalize(aggregateResult);
	}
  }

  // Create output schema
  std::vector<std::shared_ptr<arrow::Field>> fields;
  for (const auto &columnName: columnNames_) {
	auto field = inputSchema_.value()->GetFieldByName(columnName);
	fields.emplace_back(field);
  }
  for (const auto &function: aggregateFunctions_) {
	auto returnType = function->returnType();
	if (!returnType) {
	  // FIXME: if no tuple to group by, the return type is missing, here use DoubleType
	  returnType = std::make_shared<::arrow::DoubleType>();
	}
//      if (!returnType) {
//        auto emptyTupleSet = TupleSet2::make2();
//        std::shared_ptr<normal::core::message::Message>
//                tupleMessage = std::make_shared<normal::core::message::TupleMessage>(emptyTupleSet->toTupleSetV1(), this->name());
//        ctx()->tell(tupleMessage);
//
//        ctx()->notifyComplete();
//        return;
//      }
	std::shared_ptr<arrow::Field> field = arrow::field(function->alias(), returnType);
	fields.emplace_back(field);
  }
  auto schema = std::make_shared<Schema>(::arrow::schema(fields));

  // Create the group field columns
  std::vector<std::shared_ptr<arrow::Array>> groupFieldColumns;
  for (size_t c = 0; c < columnNames_.size(); c++) {
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

	for (const auto &groupAggregateResults: aggregateResults_) {
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

	  if (!arrowStatus.ok()) {
		// FIXME
		throw std::runtime_error(arrowStatus.message());
	  }
	}

	auto arrowStatus = builder->Finish(&array);
	if (!arrowStatus.ok()) {
	  // FIXME
	  throw std::runtime_error(arrowStatus.message());
	}
	groupFieldColumns.emplace_back(array);
  }

  // Create the aggregate function columns
  std::vector<std::shared_ptr<arrow::Array>> aggregateFunctionColumns;
  for (size_t c = 0; c < aggregateFunctions_.size(); ++c) {
	std::shared_ptr<arrow::Array> array;
	std::shared_ptr<::arrow::ArrayBuilder> builder;
	auto function = aggregateFunctions_.at(c);

	if (!function->returnType()) {
	  // FIXME: no tuple, double type in default
	  builder = std::make_shared<::arrow::DoubleBuilder>();
	  auto arrowStatus = builder->Finish(&array);
	} else {
	  if (function->returnType()->id() == arrow::Int32Type::type_id) {
		builder = std::make_shared<::arrow::Int32Builder>();
	  } else if (function->returnType()->id() == arrow::Int64Type::type_id) {
		builder = std::make_shared<::arrow::Int64Builder>();
	  } else if (function->returnType()->id() == arrow::DoubleType::type_id) {
		builder = std::make_shared<::arrow::DoubleBuilder>();
	  } else {
		throw std::runtime_error("Unrecognized aggregation type " + function->returnType()->name());
	  }

	  auto field = schema->getSchema()->field(c);
	  for (const auto &groupAggregateResults: aggregateResults_) {
		auto groupResults = groupAggregateResults.second;
		auto groupResultValue = groupResults.at(c);
		auto groupResultValueArrowScalar = groupResultValue->evaluate();
		auto groupResultValueScalar = Scalar::make(groupResultValueArrowScalar);

		::arrow::Status arrowStatus;
		if (function->returnType()->id() == arrow::Int32Type::type_id) {
		  auto int32Builder = std::static_pointer_cast<::arrow::Int32Builder>(builder);
		  arrowStatus = int32Builder->Append(groupResultValueScalar->value<int>());
		} else if (function->returnType()->id() == arrow::Int64Type::type_id) {
		  auto int64Builder = std::static_pointer_cast<::arrow::Int64Builder>(builder);
		  arrowStatus = int64Builder->Append(groupResultValueScalar->value<long>());
		} else if (function->returnType()->id() == arrow::DoubleType::type_id) {
		  auto doubleBuilder = std::static_pointer_cast<::arrow::DoubleBuilder>(builder);
		  arrowStatus = doubleBuilder->Append(groupResultValueScalar->value<double>());
		}

		if (!arrowStatus.ok()) {
		  // FIXME
		  throw std::runtime_error(arrowStatus.message());
		}
	  }

	  auto arrowStatus = builder->Finish(&array);
	  if (!arrowStatus.ok()) {
		// FIXME
		throw std::runtime_error(arrowStatus.message());
	  }
	}

	aggregateFunctionColumns.emplace_back(array);
  }

  std::vector<std::shared_ptr<::arrow::Array>> columns;
  columns.insert(columns.end(), groupFieldColumns.begin(), groupFieldColumns.end());
  columns.insert(columns.end(), aggregateFunctionColumns.begin(), aggregateFunctionColumns.end());

  auto table = arrow::Table::Make(schema->getSchema(), columns);

  std::shared_ptr<TupleSet2> groupedTupleSet = TupleSet2::make(table);

  return groupedTupleSet;
}

}