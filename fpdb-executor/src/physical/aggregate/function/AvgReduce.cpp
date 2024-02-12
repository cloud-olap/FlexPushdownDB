//
// Created by Yifei Yang on 1/25/22.
//

#include <fpdb/executor/physical/aggregate/function/AvgReduce.h>
#include <fpdb/executor/physical/aggregate/function/Count.h>
#include <arrow/compute/api_aggregate.h>
#include <arrow/compute/cast.h>

namespace fpdb::executor::physical::aggregate {

AvgReduce::AvgReduce(const string &outputColumnName,
                     const shared_ptr<fpdb::expression::gandiva::Expression> &expression)
  : AvgBase(AVG_REDUCE, outputColumnName, expression) {}

std::string AvgReduce::getTypeString() const {
  return "AvgReduce";
}

set<string> AvgReduce::involvedColumnNames() const {
  return {getIntermediateSumColumnName(), getIntermediateCountColumnName()};
}

tl::expected<shared_ptr<arrow::Scalar>, string> AvgReduce::computeComplete(const shared_ptr<TupleSet> &tupleSet) {
  // compute sum and count
  const auto &expAggResult = computePartial(tupleSet);
  if (!expAggResult) {
    return tl::make_unexpected(expAggResult.error());
  }
  const auto &aggResult = *expAggResult;
  const auto &expSumScalar = aggResult->get(SUM_RESULT_KEY);
  if (!expSumScalar.has_value()) {
    return tl::make_unexpected(expSumScalar.error());
  }
  const auto &expCountScalar = aggResult->get(COUNT_RESULT_KEY);
  if (!expCountScalar.has_value()) {
    return tl::make_unexpected(expCountScalar.error());
  }

  // compute average
  const auto &expAvgScalar = arrow::compute::CallFunction("divide", {*expSumScalar, *expCountScalar});
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

tl::expected<shared_ptr<AggregateResult>, string> AvgReduce::computePartial(const shared_ptr<TupleSet> &tupleSet) {
  // compute sum
  const auto &sumInputColumn = tupleSet->table()->GetColumnByName(getIntermediateSumColumnName());
  if (!sumInputColumn) {
    return tl::make_unexpected(fmt::format("AVG_INTERMEDIATE_SUM_COLUMN for {} not exist", outputColumnName_));
  }
  aggColumnDataType_ = sumInputColumn->type();
  const auto &expSumDatum = arrow::compute::Sum(sumInputColumn);
  if (!expSumDatum.ok()) {
    return tl::make_unexpected(expSumDatum.status().message());
  }
  const auto &sumScalar = (*expSumDatum).scalar();

  // compute count
  const auto &countInputColumn = tupleSet->table()->GetColumnByName(getIntermediateCountColumnName());
  if (!countInputColumn) {
    return tl::make_unexpected(fmt::format("AVG_INTERMEDIATE_COUNT_COLUMN for {} not exist", outputColumnName_));
  }
  const auto &expCountDatum = arrow::compute::Sum(countInputColumn);
  if (!expCountDatum.ok()) {
    return tl::make_unexpected(expCountDatum.status().message());
  }
  const auto &countScalar = (*expCountDatum).scalar();

  // make the aggregateResult
  auto aggregateResult = make_shared<AggregateResult>();
  aggregateResult->put(SUM_RESULT_KEY, sumScalar);
  aggregateResult->put(COUNT_RESULT_KEY, countScalar);
  return aggregateResult;
}

std::vector<std::tuple<arrow::compute::internal::Aggregate, arrow::FieldRef, std::string,
std::shared_ptr<arrow::Field>>> AvgReduce::getArrowAggregateSignatures() {
  static auto defaultScalarAggregateOptions = arrow::compute::ScalarAggregateOptions::Defaults();
  auto intermediateSumColumnName = getIntermediateSumColumnName();
  auto intermediateCountColumnName = getIntermediateCountColumnName();

  std::tuple<arrow::compute::internal::Aggregate, arrow::FieldRef, std::string, std::shared_ptr<arrow::Field>>
          sumAggregateSignature{
          {"hash_sum", &defaultScalarAggregateOptions},
          intermediateSumColumnName,
          intermediateSumColumnName,
          arrow::field(intermediateSumColumnName, aggColumnDataType_)
  };
  std::tuple<arrow::compute::internal::Aggregate, arrow::FieldRef, std::string, std::shared_ptr<arrow::Field>>
          countAggregateSignature{
          {"hash_sum", &defaultScalarAggregateOptions},
          intermediateCountColumnName,
          intermediateCountColumnName,
          arrow::field(intermediateCountColumnName, Count::defaultReturnType())
  };
  return {sumAggregateSignature, countAggregateSignature};
}

void AvgReduce::setAggColumnDataType(const std::shared_ptr<TupleSet> &tupleSet) {
  if (aggColumnDataType_ == nullptr) {
    aggColumnDataType_ = tupleSet->schema()->GetFieldByName(getIntermediateSumColumnName())->type();
  }
}

}
