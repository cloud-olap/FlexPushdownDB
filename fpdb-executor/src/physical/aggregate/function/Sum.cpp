//
// Created by Yifei Yang on 12/2/21.
//

#include <fpdb/executor/physical/aggregate/function/Sum.h>
#include <fpdb/executor/physical/aggregate/function/AggregateFunctionType.h>
#include <arrow/compute/api_aggregate.h>

namespace fpdb::executor::physical::aggregate {

Sum::Sum(const string &outputColumnName,
         const shared_ptr<fpdb::expression::gandiva::Expression> &expression)
  : AggregateFunction(SUM, outputColumnName, expression) {}

std::string Sum::getTypeString() const {
  return "Sum";
}

tl::expected<shared_ptr<arrow::Scalar>, string> Sum::computeComplete(const shared_ptr<TupleSet> &tupleSet) {
  // evaluate the expression to get input of aggregation
  const auto &expAggChunkedArray = evaluateExpr(tupleSet);
  if (!expAggChunkedArray.has_value()) {
    return tl::make_unexpected(expAggChunkedArray.error());
  }

  // compute the aggregation
  const auto &expResultDatum = arrow::compute::Sum(*expAggChunkedArray);
  if (!expResultDatum.ok()) {
    return tl::make_unexpected(expResultDatum.status().message());
  }
  return (*expResultDatum).scalar();
}

tl::expected<shared_ptr<AggregateResult>, string> Sum::computePartial(const shared_ptr<TupleSet> &tupleSet) {
  // compute the result scalar
  const auto &expResultScalar = computeComplete(tupleSet);
  if (!expResultScalar) {
    return tl::make_unexpected(expResultScalar.error());
  }

  // make the aggregateResult
  auto aggregateResult = make_shared<AggregateResult>();
  aggregateResult->put(SUM_RESULT_KEY, *expResultScalar);
  return aggregateResult;
}

tl::expected<shared_ptr<arrow::Scalar>, string>
Sum::finalize(const vector<shared_ptr<AggregateResult>> &aggregateResults) {
  // build aggregate input array
  const auto expFinalizeInputArray = buildFinalizeInputArray(aggregateResults, SUM_RESULT_KEY, returnType());
  if (!expFinalizeInputArray) {
    return tl::make_unexpected(expFinalizeInputArray.error());
  }

  // compute the final aggregation
  const auto &expFinalResultScalar = arrow::compute::Sum(expFinalizeInputArray.value());
  if (!expFinalResultScalar.ok()) {
    return tl::make_unexpected(expFinalResultScalar.status().message());
  }
  return (*expFinalResultScalar).scalar();
}

std::vector<std::tuple<arrow::compute::internal::Aggregate, arrow::FieldRef, std::string,
std::shared_ptr<arrow::Field>>> Sum::getArrowAggregateSignatures() {
  static auto defaultScalarAggregateOptions = arrow::compute::ScalarAggregateOptions::Defaults();
  std::tuple<arrow::compute::internal::Aggregate, arrow::FieldRef, std::string, std::shared_ptr<arrow::Field>>
          aggregateSignature{
          {"hash_sum", &defaultScalarAggregateOptions},
          getAggregateInputColumnName(),
          outputColumnName_,
          arrow::field(outputColumnName_, returnType())
  };
  return {aggregateSignature};
}

}
