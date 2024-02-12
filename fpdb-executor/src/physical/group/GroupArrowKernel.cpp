//
// Created by Yifei Yang on 4/20/22.
//

#include <fpdb/executor/physical/group/GroupArrowKernel.h>
#include <fpdb/executor/physical/aggregate/function/AvgReduce.h>
#include <fpdb/tuple/arrow/exec/DummyNode.h>
#include <arrow/compute/exec/exec_plan.h>

namespace fpdb::executor::physical::group {

GroupArrowKernel::GroupArrowKernel(const std::vector<std::string> &groupColumnNames,
                                   const std::vector<std::shared_ptr<AggregateFunction>> &aggregateFunctions):
  GroupAbstractKernel(GroupKernelType::GROUP_ARROW_KERNEL, groupColumnNames, aggregateFunctions) {}

tl::expected<void, std::string> GroupArrowKernel::group(const std::shared_ptr<TupleSet> &tupleSet) {
  // evaluate expr
  auto expEvaluatedTupleSet = evaluateExpr(tupleSet);
  if (!expEvaluatedTupleSet.has_value()) {
    return tl::make_unexpected(expEvaluatedTupleSet.error());
  }
  auto evaluatedTupleSet = *expEvaluatedTupleSet;

  // prepare for group operation, i.e. make outputSchema and aggregateNodeOptions
  makeOutputSchema(evaluatedTupleSet->schema());

  // skip processing if tupleSet is empty
  if (tupleSet->numRows() == 0) {
    return {};
  }

  // group evaluated and buffer result
  auto expGroupedTupleSet = doGroup(evaluatedTupleSet);
  if (!expGroupedTupleSet.has_value()) {
    return tl::make_unexpected(expGroupedTupleSet.error());
  }

  return {};
}

tl::expected<shared_ptr<TupleSet>, std::string> GroupArrowKernel::finalise() {
  if (!outputSchema_.has_value()) {
    return tl::make_unexpected("Output schema not made yet");
  }
  std::shared_ptr<TupleSet> outputTupleSet;

  // need to specially handle the case when the input has no rows
  if (!arrowExecPlanSuite_.has_value()) {
    outputTupleSet = TupleSet::make(*outputSchema_);
  } else {
    // finish input
    arrowExecPlanSuite_->aggregateNode_->InputFinished(arrowExecPlanSuite_->dummyNode_,
                                                       arrowExecPlanSuite_->numInputBatches_);

    // collect result table from sink node
    auto sinkReader = arrow::compute::MakeGeneratorReader(*outputSchema_,
                                                          *arrowExecPlanSuite_->sinkGen_,
                                                          arrowExecPlanSuite_->execPlan_->exec_context()->memory_pool());
    auto expOutputTable = arrow::Table::FromRecordBatchReader(sinkReader.get());
    if (!expOutputTable.ok()) {
      return tl::make_unexpected(expOutputTable.status().message());
    }
    outputTupleSet = TupleSet::make(*expOutputTable);
  }

  // for functions which are Avg or AvgReduce, we need to divide intermediate sum column by intermediate count column
  return finalizeAvg(outputTupleSet);
}

tl::expected<std::shared_ptr<TupleSet>, std::string>
GroupArrowKernel::evaluateExpr(const std::shared_ptr<TupleSet> &tupleSet) {
  // group columns
  auto expGroupColumns = getGroupColumns(tupleSet);
  if (!expGroupColumns.has_value()) {
    return tl::make_unexpected(expGroupColumns.error());
  }
  auto outputFields = expGroupColumns->first;
  auto outputColumns = expGroupColumns->second;

  // aggregate columns
  for (uint i = 0; i < aggregateFunctions_.size(); ++i) {
    auto function = aggregateFunctions_[i];

    // for AVG_REDUCE, just return the original tupleSet
    // because all columns are needed and there is no expr
    // need to specially handle AVG_REDUCE because it has two aggregate columns and no expr
    if (function->getType() == AggregateFunctionType::AVG_REDUCE) {
      auto avgFunction = std::static_pointer_cast<AvgReduce>(function);
      // intermediate sum column
      auto expIntermediateSumColumn = avgFunction->getIntermediateSumColumn(tupleSet);
      if (!expIntermediateSumColumn.has_value()) {
        return tl::make_unexpected(expIntermediateSumColumn.error());
      }
      outputFields.emplace_back((*expIntermediateSumColumn).first);
      outputColumns.emplace_back((*expIntermediateSumColumn).second);
      // intermediate count column
      auto expIntermediateCountColumn = avgFunction->getIntermediateCountColumn(tupleSet);
      if (!expIntermediateCountColumn.has_value()) {
        return tl::make_unexpected(expIntermediateCountColumn.error());
      }
      outputFields.emplace_back((*expIntermediateCountColumn).first);
      outputColumns.emplace_back((*expIntermediateCountColumn).second);
      // need to set explicitly
      avgFunction->setAggColumnDataType(tupleSet);
    }

    else {
      std::shared_ptr<arrow::ChunkedArray> outputColumn;
      // need to specially handel count(*) because it has no expr
      if (function->getType() == AggregateFunctionType::COUNT && function->getExpression() == nullptr) {
        outputColumn = tupleSet->table()->column(0);
      } else {
        auto expOutputColumn = function->evaluateExpr(tupleSet);
        if (!expOutputColumn.has_value()) {
          return tl::make_unexpected(expOutputColumn.error());
        }
        outputColumn = *expOutputColumn;
      }
      outputColumns.emplace_back(outputColumn);
      outputFields.emplace_back(arrow::field(function->getAggregateInputColumnName(), outputColumn->type()));
    }
  }

  return TupleSet::make(arrow::schema(outputFields), outputColumns);
}

tl::expected<void, std::string> GroupArrowKernel::doGroup(const std::shared_ptr<TupleSet> &tupleSet) {
  // make arrow exec plan if not yet
  makeArrowExecPlan(tupleSet->schema());

  // read tupleSet into batches
  auto reader = std::make_shared<arrow::TableBatchReader>(*tupleSet->table());
  auto recordBatchReadResult = reader->Next();
  reader->set_chunksize(DefaultChunkSize);
  if (!recordBatchReadResult.ok()) {
    return tl::make_unexpected(recordBatchReadResult.status().message());
  }
  auto recordBatch = *recordBatchReadResult;

  // process batches
  while (recordBatch) {
    ++arrowExecPlanSuite_->numInputBatches_;
    arrow::compute::ExecBatch execBatch(*recordBatch);
    arrowExecPlanSuite_->aggregateNode_->InputReceived(arrowExecPlanSuite_->dummyNode_, execBatch);

    // next batch
    recordBatchReadResult = reader->Next();
    if (!recordBatchReadResult.ok())
      return tl::make_unexpected(recordBatchReadResult.status().message());
    recordBatch = *recordBatchReadResult;
  }

  return {};
}

tl::expected<std::shared_ptr<TupleSet>, std::string>
GroupArrowKernel::finalizeAvg(const std::shared_ptr<TupleSet> &tupleSet) {
  // group columns
  auto expGroupColumns = getGroupColumns(tupleSet);
  if (!expGroupColumns.has_value()) {
    return tl::make_unexpected(expGroupColumns.error());
  }
  auto outputFields = expGroupColumns->first;
  auto outputColumns = expGroupColumns->second;

  for (const auto &function: aggregateFunctions_) {
    // not an Avg or AvgReduce function
    if (function->getType() != AggregateFunctionType::AVG && function->getType() != AggregateFunctionType::AVG_REDUCE) {
      auto outputColumnName = function->getOutputColumnName();

      auto aggregateField = tupleSet->schema()->GetFieldByName(outputColumnName);
      if (aggregateField == nullptr) {
        return tl::make_unexpected(
                fmt::format("Aggregate output field '{}' not found in finalized table", outputColumnName));
      }
      outputFields.emplace_back(aggregateField);

      auto aggregateColumn = tupleSet->table()->GetColumnByName(outputColumnName);
      if (aggregateColumn == nullptr) {
        return tl::make_unexpected(
                fmt::format("Aggregate output column '{}' not found in finalized table", outputColumnName));
      }
      outputColumns.emplace_back(aggregateColumn);
    }

    // is an Avg or AvgReduce function
    else {
      auto avgFunction = std::static_pointer_cast<AvgBase>(function);
      outputFields.emplace_back(arrow::field(avgFunction->getOutputColumnName(), avgFunction->returnType()));

      auto expOutputColumn = avgFunction->finalize(tupleSet);
      if (!expOutputColumn.has_value()) {
        return tl::make_unexpected(expOutputColumn.error());
      }
      outputColumns.emplace_back(*expOutputColumn);
    }
  }

  return TupleSet::make(arrow::schema(outputFields), outputColumns);
}

tl::expected<void, std::string> GroupArrowKernel::makeOutputSchema(const std::shared_ptr<arrow::Schema> &schema) {
  if (outputSchema_.has_value()) {
    return {};
  }

  // group fields
  arrow::FieldVector groupFields;
  std::vector<arrow::FieldRef> keys;
  for (const auto &groupColumnName: groupColumnNames_) {
    auto groupField = schema->GetFieldByName(groupColumnName);
    if (groupField == nullptr) {
      return tl::make_unexpected(fmt::format("Group field '{}' not found in input schema", groupColumnName));
    }
    groupFields.emplace_back(groupField);
    keys.emplace_back(arrow::FieldRef(groupColumnName));
  }

  // aggregateNodeOptions and aggregate fields
  std::vector<arrow::compute::internal::Aggregate> aggregates;
  std::vector<arrow::FieldRef> targets;
  std::vector<std::string> names;
  arrow::FieldVector aggregateFields;

  for (const auto &function: aggregateFunctions_) {
    auto aggregateSignatures = function->getArrowAggregateSignatures();
    for (const auto &aggregateSignature: aggregateSignatures) {
      aggregates.emplace_back(std::get<0>(aggregateSignature));
      targets.emplace_back(std::get<1>(aggregateSignature));
      names.emplace_back(std::get<2>(aggregateSignature));
      aggregateFields.emplace_back(std::get<3>(aggregateSignature));
    }
  }
  aggregateNodeOptions_ = arrow::compute::AggregateNodeOptions(aggregates, targets, names, keys);

  // output schema
  arrow::FieldVector outputFields;
  outputFields.insert(outputFields.end(), aggregateFields.begin(), aggregateFields.end());
  outputFields.insert(outputFields.end(), groupFields.begin(), groupFields.end());
  outputSchema_ = arrow::schema(outputFields);

  return {};
}

tl::expected<void, std::string> GroupArrowKernel::makeArrowExecPlan(const std::shared_ptr<arrow::Schema> &schema) {
  if (arrowExecPlanSuite_.has_value()) {
    return {};
  }

  // initialize
  auto execContext = std::make_shared<arrow::compute::ExecContext>(arrow::default_memory_pool());

  // exec plan
  auto expExecPlan = arrow::compute::ExecPlan::Make(execContext.get());
  if (!expExecPlan.ok()) {
    return tl::make_unexpected(expExecPlan.status().message());
  }
  auto execPlan = *expExecPlan;

  // dummy node at the beginning
  auto expDummyNode = arrow::compute::DummyNode::Make(execPlan.get(), schema);
  if (!expDummyNode.ok()) {
    return tl::make_unexpected(expDummyNode.status().message());
  }
  auto dummyNode = *expDummyNode;

  // hash-aggregate node
  auto expAggregateNode = arrow::compute::MakeExecNode("aggregate", execPlan.get(), {dummyNode},
                                                       *aggregateNodeOptions_);
  if (!expAggregateNode.ok()) {
    return tl::make_unexpected(expAggregateNode.status().message());
  }
  auto aggregateNode = *expAggregateNode;

  // sink node at the end
  auto sinkGen = std::make_shared<arrow::AsyncGenerator<arrow::util::optional<arrow::compute::ExecBatch>>>();
  auto expSinkNode = arrow::compute::MakeExecNode("sink", execPlan.get(), {aggregateNode},
                                                  arrow::compute::SinkNodeOptions{sinkGen.get()});
  if (!expSinkNode.ok()) {
    return tl::make_unexpected(expSinkNode.status().message());
  }
  auto sinkNode = *expSinkNode;

  // save variables, start aggregate node and sink node
  arrowExecPlanSuite_ = GroupArrowExecPlanSuite{execContext, execPlan, dummyNode, aggregateNode, sinkNode, sinkGen, 0};
  auto status = aggregateNode->StartProducing();
  if (!status.ok()) {
    return tl::make_unexpected(status.message());
  }
  status = sinkNode->StartProducing();
  if (!status.ok()) {
    return tl::make_unexpected(status.message());
  }

  return {};
}

tl::expected<std::pair<arrow::FieldVector, arrow::ChunkedArrayVector>, std::string>
GroupArrowKernel::getGroupColumns(const std::shared_ptr<TupleSet> &tupleSet) {
  arrow::FieldVector groupFields;
  arrow::ChunkedArrayVector groupColumns;

  for (const auto &groupColumnName: groupColumnNames_) {
    auto groupField = tupleSet->schema()->GetFieldByName(groupColumnName);
    if (groupField == nullptr) {
      return tl::make_unexpected(fmt::format("Group field '{}' not found in input table", groupColumnName));
    }
    groupFields.emplace_back(groupField);

    auto groupColumn = tupleSet->table()->GetColumnByName(groupColumnName);
    if (groupColumn == nullptr) {
      return tl::make_unexpected(fmt::format("Group column '{}' not found in input table", groupColumnName));
    }
    groupColumns.emplace_back(groupColumn);
  }

  return std::make_pair(groupFields, groupColumns);
}

void GroupArrowKernel::clear() {
  arrowExecPlanSuite_.reset();
}

}
