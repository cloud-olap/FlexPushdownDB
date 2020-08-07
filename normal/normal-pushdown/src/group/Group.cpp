//
// Created by matt on 13/5/20.
//

#include <normal/pushdown/group/Group.h>
#include <normal/tuple/TupleSet2.h>

#include <normal/pushdown/group/GroupKey.h>
#include <normal/tuple/ArrayAppender.h>

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

//  // empty tuple
//  if (tupleSet->numRows() == 0) {
//    return;
//  }

  // Set the input schema if not yet set
  cacheInputSchema(*message.tuples());

  // Fill the groupedTuples using record batch
  auto startTime = std::chrono::steady_clock::now();
  std::shared_ptr<::arrow::Table> table;
  if (tupleSet->getArrowTable().has_value()) {
    table = tupleSet->getArrowTable().value();
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

  // temporary grouped arrays
  std::unordered_map<std::shared_ptr<GroupKey>, std::shared_ptr<std::vector<std::shared_ptr<ArrayAppender>>>, GroupKeyPointerHash, GroupKeyPointerPredicate> tempGroupedArrays;

  while (recordBatch) {
    std::vector<std::shared_ptr<arrow::Array>> batchArrays;
    for (int col_id = 0; col_id < recordBatch->num_columns(); ++col_id) {
      batchArrays.emplace_back(recordBatch->column(col_id));
    }
    auto batchTupleSet = TupleSet2::make(arrowSchema, batchArrays);

    // process for each tuple
    for (int row_id = 0; row_id < batchTupleSet->numRows(); ++row_id) {
      // build the group key
      auto groupKey = GroupKey::make();
      for (const auto &columnName: columnNames_) {
//        auto groupColumn = recordBatch->GetColumnByName(columnName);
//        if (groupColumn->type()->id() == arrow::Int64Type::type_id) {
//          auto typedColumn = std::static_pointer_cast<::arrow::Int64Array>(groupColumn);
//          auto value = ::arrow::MakeScalar(typedColumn->Value(row_id));
//          auto scalar = std::make_shared<normal::tuple::Scalar>(value);
//          groupKey->append(columnName, scalar);
//        } else if (groupColumn->type()->id() == arrow::DoubleType::type_id) {
//          auto typedColumn = std::static_pointer_cast<::arrow::DoubleArray>(groupColumn);
//          auto value = ::arrow::MakeScalar(typedColumn->Value(row_id));
//          auto scalar = std::make_shared<normal::tuple::Scalar>(value);
//        } else if (groupColumn->type()->id() == arrow::StringType::type_id) {
//          auto typedColumn = std::static_pointer_cast<::arrow::StringArray>(groupColumn);
//          auto value = ::arrow::MakeScalar(typedColumn->GetString(row_id));
//          auto scalar = std::make_shared<normal::tuple::Scalar>(::arrow::MakeScalar(1));
//        }

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

      // get or initialize temporary grouped arrays (arrayAppenders)
      std::shared_ptr<std::vector<std::shared_ptr<ArrayAppender>>> arrayAppenders;
      auto currentArrayAppendersIt = tempGroupedArrays.find(groupKey);
      if (currentArrayAppendersIt == tempGroupedArrays.end()) {
        arrayAppenders = std::make_shared<std::vector<std::shared_ptr<ArrayAppender>>>();
        for (int col_id = 0; col_id < recordBatch->num_columns(); ++col_id) {
          auto expectedAppender = ArrayAppender::make(arrowSchema->field(col_id)->type(), recordBatch->num_rows());
          if (!expectedAppender.has_value()) {
            throw std::runtime_error(expectedAppender.error());
          }
          arrayAppenders->emplace_back(expectedAppender.value());
        }
        tempGroupedArrays.emplace(groupKey, arrayAppenders);
      } else {
        arrayAppenders = currentArrayAppendersIt->second;
      }

      // append values of the current tuple
      for (int col_id = 0; col_id < recordBatch->num_columns(); ++col_id) {
        arrayAppenders->at(col_id)->appendValue(recordBatch->column(col_id), row_id);
      }
    }

    // next batch
    recordBatchResult = reader.Next();
    if (!recordBatchResult.ok()) {
      throw std::runtime_error(recordBatchResult.status().message());
    }
    recordBatch = *recordBatchResult;
  }

  // finalize temporary grouped arrays
  for (auto const &arrayAppendersIt: tempGroupedArrays) {
    auto groupKey = arrayAppendersIt.first;
    auto arrayAppenders = arrayAppendersIt.second;
    auto arrays = std::vector<std::shared_ptr<arrow::Array>>();
    for (auto const &arrayAppender: *arrayAppenders) {
      arrays.emplace_back(arrayAppender->finalize().value());
    }
    groupedTuples_.emplace(groupKey, TupleSet2::make(arrowSchema, arrays));
  }
  auto endTime = std::chrono::steady_clock::now();
  SPDLOG_INFO("Group key time: {}, size: {}",
          std::chrono::duration_cast<std::chrono::nanoseconds>(endTime - startTime).count(),
          tupleSet->numRows());
  startTime = endTime;

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
  endTime = std::chrono::steady_clock::now();
  SPDLOG_INFO("Aggregate time: {}, size: {}",
          std::chrono::duration_cast<std::chrono::nanoseconds>(endTime - startTime).count(),
          tupleSet->numRows());

  // clear computed groupedTuples
  groupedTuples_.clear();
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
      std::shared_ptr<::arrow::ArrayBuilder> builder;
      auto function = aggregateFunctions_->at(c);

      if (!function->returnType()) {
        // FIXME: no tuple, double type in default
        builder = std::make_shared<::arrow::DoubleBuilder>();
        auto arrowStatus = builder->Finish(&array);
      }

      else {
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
    columns.insert (columns.end(), groupFieldColumns.begin(), groupFieldColumns.end());
    columns.insert (columns.end(), aggregateFunctionColumns.begin(), aggregateFunctionColumns.end());

    auto table = arrow::Table::Make(schema->getSchema(), columns);

    const std::shared_ptr<TupleSet> &groupedTupleSet = TupleSet::make(table);

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
