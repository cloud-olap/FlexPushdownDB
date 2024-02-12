//
// Created by Yifei Yang on 1/26/22.
//

#include <fpdb/executor/physical/aggregate/function/AvgBase.h>
#include <fpdb/executor/physical/aggregate/function/Count.h>
#include <fpdb/plan/prephysical/AggregatePrePFunction.h>
#include <arrow/compute/api_aggregate.h>
#include <arrow/compute/cast.h>

using namespace fpdb::plan::prephysical;

namespace fpdb::executor::physical::aggregate {

AvgBase::AvgBase(AggregateFunctionType type,
                 const string &outputColumnName,
                 const shared_ptr<fpdb::expression::gandiva::Expression> &expression) :
  AggregateFunction(type, outputColumnName, expression) {}

shared_ptr<arrow::DataType> AvgBase::returnType() const {
  return defaultReturnType();
}

shared_ptr<arrow::DataType> AvgBase::defaultReturnType() {
  return arrow::float64();
}

tl::expected<shared_ptr<arrow::Scalar>, string>
AvgBase::finalize(const vector<shared_ptr<AggregateResult>> &aggregateResults) {
  // build aggregate input arrays
  const auto expFinalizeSumInputArray = buildFinalizeInputArray(aggregateResults,
                                                                SUM_RESULT_KEY,
                                                                aggColumnDataType_);
  if (!expFinalizeSumInputArray) {
    return tl::make_unexpected(expFinalizeSumInputArray.error());
  }
  const auto expFinalizeCountInputArray = buildFinalizeInputArray(aggregateResults,
                                                                  COUNT_RESULT_KEY,
                                                                  Count::defaultReturnType());
  if (!expFinalizeCountInputArray) {
    return tl::make_unexpected(expFinalizeCountInputArray.error());
  }

  // compute final sum and count
  const auto &expFinalSumScalar = arrow::compute::Sum(expFinalizeSumInputArray.value());
  if (!expFinalSumScalar.ok()) {
    return tl::make_unexpected(expFinalSumScalar.status().message());
  }
  const auto &finalSumScalar = (*expFinalSumScalar).scalar();
  const auto &expFinalCountScalar = arrow::compute::Sum(expFinalizeCountInputArray.value());
  if (!expFinalCountScalar.ok()) {
    return tl::make_unexpected(expFinalCountScalar.status().message());
  }
  const auto &finalCountScalar = (*expFinalCountScalar).scalar();

  // compute average
  const auto &expAvgScalar = arrow::compute::CallFunction("divide", {finalSumScalar, finalCountScalar});
  if (!expAvgScalar.ok()) {
    return tl::make_unexpected(expAvgScalar.status().message());
  }
  const auto &avgScalar = (*expAvgScalar).scalar();

  // cast to float64 to avoid implicit cast at downstream
  const auto &expCastScalar = arrow::compute::Cast(avgScalar, returnType());
  if (!expCastScalar.ok()) {
    return tl::make_unexpected(expCastScalar.status().message());
  }
  return (*expCastScalar).scalar();
}

tl::expected<shared_ptr<arrow::ChunkedArray>, std::string> AvgBase::finalize(const shared_ptr<TupleSet> &tupleSet) {
  // intermediate sum column
  auto intermediateSumColumn = tupleSet->table()->GetColumnByName(getIntermediateSumColumnName());
  if (intermediateSumColumn == nullptr) {
    return tl::make_unexpected(
            fmt::format("Intermediate sum column not found: '{}'", getIntermediateSumColumnName()));
  }

  // intermediate count column
  auto intermediateCountColumn = tupleSet->table()->GetColumnByName(getIntermediateCountColumnName());
  if (intermediateCountColumn == nullptr) {
    return tl::make_unexpected(
            fmt::format("Intermediate count column not found: '{}'", getIntermediateCountColumnName()));
  }

  // compute average
  auto expAvgColumn = arrow::compute::CallFunction("divide", {intermediateSumColumn, intermediateCountColumn});
  if (!expAvgColumn.ok()) {
    return tl::make_unexpected(expAvgColumn.status().message());
  }
  auto avgColumn = (*expAvgColumn).chunked_array();

  // cast to float64 to avoid implicit cast at downstream
  auto expCastAvgColumn = arrow::compute::Cast(avgColumn, returnType());
  if (!expCastAvgColumn.ok()) {
    return tl::make_unexpected(expCastAvgColumn.status().message());
  }
  return (*expCastAvgColumn).chunked_array();
}

tl::expected<pair<shared_ptr<arrow::Field>, shared_ptr<arrow::ChunkedArray>>, std::string>
AvgBase::getIntermediateSumColumn(const shared_ptr<TupleSet> &tupleSet) const {
  auto expColumn = tupleSet->getColumnByName(getIntermediateSumColumnName());
  if (!expColumn.has_value()) {
    return tl::make_unexpected(expColumn.error());
  }
  auto column = *expColumn;
  return make_pair(make_shared<arrow::Field>(column->getName(), column->type()),
                   column->getArrowArray());
}

tl::expected<pair<shared_ptr<arrow::Field>, shared_ptr<arrow::ChunkedArray>>, std::string>
AvgBase::getIntermediateCountColumn(const shared_ptr<TupleSet> &tupleSet) const {
  auto expColumn = tupleSet->getColumnByName(getIntermediateCountColumnName());
  if (!expColumn.has_value()) {
    return tl::make_unexpected(expColumn.error());
  }
  auto column = *expColumn;
  return make_pair(make_shared<arrow::Field>(column->getName(), column->type()),
                   column->getArrowArray());
}

std::string AvgBase::getIntermediateSumColumnName() const {
  return AggregatePrePFunction::AVG_INTERMEDIATE_SUM_COLUMN_PREFIX + outputColumnName_;
}

std::string AvgBase::getIntermediateCountColumnName() const {
  return AggregatePrePFunction::AVG_INTERMEDIATE_COUNT_COLUMN_PREFIX + outputColumnName_;
}

}
