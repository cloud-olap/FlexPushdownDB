//
// Created by Yifei Yang on 1/25/22.
//

#include <fpdb/executor/physical/aggregate/function/Avg.h>
#include <fpdb/executor/physical/aggregate/function/AggregateFunctionType.h>
#include <fpdb/executor/physical/aggregate/function/Count.h>
#include <arrow/compute/api_aggregate.h>
#include <arrow/compute/cast.h>

namespace fpdb::executor::physical::aggregate {

Avg::Avg(const string &outputColumnName,
         const shared_ptr<fpdb::expression::gandiva::Expression> &expression)
  : AvgBase(AVG, outputColumnName, expression) {}

std::string Avg::getTypeString() const {
  return "Avg";
}

tl::expected<shared_ptr<arrow::Scalar>, string> Avg::computeComplete(const shared_ptr<TupleSet> &tupleSet) {
  // evaluate the expression to get input of aggregation
  const auto &expAggChunkedArray = evaluateExpr(tupleSet);
  if (!expAggChunkedArray.has_value()) {
    return tl::make_unexpected(expAggChunkedArray.error());
  }

  // compute avg
  const auto &expResultDatum = arrow::compute::Mean(*expAggChunkedArray);
  if (!expResultDatum.ok()) {
    return tl::make_unexpected(expResultDatum.status().message());
  }
  const auto &avgScalar = (*expResultDatum).scalar();

  // cast to float64 to avoid implicit cast at downstream
  const auto &expCastScalar = arrow::compute::Cast(avgScalar, returnType());
  if (!expCastScalar.ok()) {
    return tl::make_unexpected(expCastScalar.status().message());
  }
  return (*expCastScalar).scalar();
}

tl::expected<shared_ptr<AggregateResult>, string> Avg::computePartial(const shared_ptr<TupleSet> &tupleSet) {
  // evaluate the expression to get input of aggregation
  const auto &expAggChunkedArray = evaluateExpr(tupleSet);
  if (!expAggChunkedArray.has_value()) {
    return tl::make_unexpected(expAggChunkedArray.error());
  }
  const auto &aggChunkedArray = *expAggChunkedArray;

  // compute sum
  const auto &expSumDatum = arrow::compute::Sum(aggChunkedArray);
  if (!expSumDatum.ok()) {
    return tl::make_unexpected(expSumDatum.status().message());
  }
  const auto &sumScalar = (*expSumDatum).scalar();

  // compute count
  const auto &expCountDatum = arrow::compute::Count(*expAggChunkedArray);
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
std::shared_ptr<arrow::Field>>> Avg::getArrowAggregateSignatures() {
  static auto defaultScalarAggregateOptions = arrow::compute::ScalarAggregateOptions::Defaults();
  static auto defaultCountOptions = arrow::compute::CountOptions::Defaults();
  auto aggregateInputColumnName = getAggregateInputColumnName();
  auto intermediateSumColumnName = getIntermediateSumColumnName();
  auto intermediateCountColumnName = getIntermediateCountColumnName();

  std::tuple<arrow::compute::internal::Aggregate, arrow::FieldRef, std::string, std::shared_ptr<arrow::Field>>
          sumAggregateSignature{
          {"hash_sum", &defaultScalarAggregateOptions},
          aggregateInputColumnName,
          intermediateSumColumnName,
          arrow::field(intermediateSumColumnName, aggColumnDataType_)
  };
  std::tuple<arrow::compute::internal::Aggregate, arrow::FieldRef, std::string, std::shared_ptr<arrow::Field>>
          countAggregateSignature{
          {"hash_count", &defaultCountOptions},
          aggregateInputColumnName,
          intermediateCountColumnName,
          arrow::field(intermediateCountColumnName, Count::defaultReturnType())
  };
  return {sumAggregateSignature, countAggregateSignature};
}

}
