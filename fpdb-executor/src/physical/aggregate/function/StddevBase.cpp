//
// Created by Yifei Yang on 12/10/24.
//

#include <fpdb/executor/physical/aggregate/function/StddevBase.h>
#include <fpdb/plan/prephysical/AggregatePrePFunction.h>
#include <arrow/compute/api_aggregate.h>
#include <arrow/compute/cast.h>

using namespace fpdb::plan::prephysical;

namespace fpdb::executor::physical::aggregate {

StddevBase::StddevBase(StddevType stddevType,
                       AggregateFunctionType funcType,
                       const string &outputColumnName,
                       const shared_ptr<fpdb::expression::gandiva::Expression> &expression) :
  AggregateFunction(funcType, outputColumnName, expression),
  stddevType_(stddevType) {}

shared_ptr<arrow::DataType> StddevBase::returnType() const {
  return defaultReturnType();
}

shared_ptr<arrow::DataType> StddevBase::defaultReturnType() {
  return arrow::float64();
}

tl::expected<shared_ptr<arrow::Scalar>, string>
StddevBase::finalize(const vector<shared_ptr<AggregateResult>> &) {
  return tl::make_unexpected("Scalar finalize for StddevBase is unimplemented");
}

tl::expected<shared_ptr<arrow::ChunkedArray>, std::string>
StddevBase::finalize(const shared_ptr<TupleSet> &tupleSet) {
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

  // intermediate sum of squares column
  auto intermediateSumOfSquaresColumn = tupleSet->table()->GetColumnByName(getIntermediateSumOfSquaresColumnName());
  if (intermediateSumOfSquaresColumn == nullptr) {
    return tl::make_unexpected(
            fmt::format("Intermediate sum of squares column not found: '{}'", getIntermediateSumOfSquaresColumnName()));
  }

  // compute standard deviation
  auto expR1 = arrow::compute::CallFunction("multiply", {intermediateSumColumn, intermediateSumColumn});
  if (!expR1.ok()) {
    return tl::make_unexpected(expR1.status().message());
  }
  auto r1 = (*expR1).chunked_array();
  auto expCastR1 = arrow::compute::Cast(r1, arrow::float64());
  if (!expCastR1.ok()) {
    return tl::make_unexpected(expCastR1.status().message());
  }
  r1 = (*expCastR1).chunked_array();

  auto expR2 = arrow::compute::CallFunction("divide", {r1, intermediateCountColumn});
  if (!expR2.ok()) {
    return tl::make_unexpected(expR2.status().message());
  }
  auto r2 = (*expR2).chunked_array();

  auto expR3 = arrow::compute::CallFunction("subtract", {intermediateSumOfSquaresColumn, r2});
  if (!expR3.ok()) {
    return tl::make_unexpected(expR3.status().message());
  }
  auto r3 = (*expR3).chunked_array();

  // denominate is different between `STDDEV_SAMP` and `STDDEV_POP`
  auto r4_0 = intermediateCountColumn;
  if (stddevType_ == StddevType::SAMP) {
    auto expR4_0 = arrow::compute::CallFunction("subtract", {r4_0, arrow::MakeScalar(1)});
    if (!expR4_0.ok()) {
      return tl::make_unexpected(expR4_0.status().message());
    }
    r4_0 = (*expR4_0).chunked_array();
  }

  auto expR4 = arrow::compute::CallFunction("divide", {r3, r4_0});
  if (!expR4.ok()) {
    return tl::make_unexpected(expR4.status().message());
  }
  auto r4 = (*expR4).chunked_array();

  auto expR5 = arrow::compute::CallFunction("sqrt", {r4});
  if (!expR5.ok()) {
    return tl::make_unexpected(expR5.status().message());
  }
  auto r5 = (*expR5).chunked_array();
  auto expCastR5 = arrow::compute::Cast(r5, returnType());
  if (!expCastR5.ok()) {
    return tl::make_unexpected(expCastR5.status().message());
  }
  return (*expCastR5).chunked_array();
}

tl::expected<pair<shared_ptr<arrow::Field>, shared_ptr<arrow::ChunkedArray>>, std::string>
StddevBase::getIntermediateSumColumn(const shared_ptr<TupleSet> &tupleSet) const {
  auto expColumn = tupleSet->getColumnByName(getIntermediateSumColumnName());
  if (!expColumn.has_value()) {
    return tl::make_unexpected(expColumn.error());
  }
  auto column = *expColumn;
  return make_pair(make_shared<arrow::Field>(column->getName(), column->type()),
                   column->getArrowArray());
}

tl::expected<pair<shared_ptr<arrow::Field>, shared_ptr<arrow::ChunkedArray>>, std::string>
StddevBase::getIntermediateCountColumn(const shared_ptr<TupleSet> &tupleSet) const {
  auto expColumn = tupleSet->getColumnByName(getIntermediateCountColumnName());
  if (!expColumn.has_value()) {
    return tl::make_unexpected(expColumn.error());
  }
  auto column = *expColumn;
  return make_pair(make_shared<arrow::Field>(column->getName(), column->type()),
                   column->getArrowArray());
}

tl::expected<pair<shared_ptr<arrow::Field>, shared_ptr<arrow::ChunkedArray>>, std::string>
StddevBase::getIntermediateSumOfSquaresColumn(const shared_ptr<TupleSet> &tupleSet) const {
  auto expColumn = tupleSet->getColumnByName(getIntermediateSumOfSquaresColumnName());
  if (!expColumn.has_value()) {
    return tl::make_unexpected(expColumn.error());
  }
  auto column = *expColumn;
  return make_pair(make_shared<arrow::Field>(column->getName(), column->type()),
                   column->getArrowArray());
}

std::string StddevBase::getIntermediateSumColumnName() const {
  return AggregatePrePFunction::STDDEV_INTERMEDIATE_SUM_COLUMN_PREFIX + outputColumnName_;
}

std::string StddevBase::getIntermediateCountColumnName() const {
  return AggregatePrePFunction::STDDEV_INTERMEDIATE_COUNT_COLUMN_PREFIX + outputColumnName_;
}

std::string StddevBase::getIntermediateSumOfSquaresColumnName() const {
  return AggregatePrePFunction::STDDEV_INTERMEDIATE_SUM_OF_SQUARES_COLUMN_PREFIX + outputColumnName_;
}

StddevType StddevBase::getStddevType() const {
  return stddevType_;
}

}
