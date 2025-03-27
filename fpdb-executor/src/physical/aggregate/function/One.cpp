//
// Created by Yifei Yang on 12/9/24.
//

#include <fpdb/executor/physical/aggregate/function/One.h>
#include <fpdb/executor/physical/aggregate/function/AggregateFunctionType.h>
#include <arrow/compute/api_aggregate.h>

namespace fpdb::executor::physical::aggregate {

One::One(const string &outputColumnName,
         const shared_ptr<fpdb::expression::gandiva::Expression> &expression)
  : AggregateFunction(ONE, outputColumnName, expression) {}

std::string One::getTypeString() const {
  return "One";
}

tl::expected<shared_ptr<arrow::Scalar>, string> One::computeComplete(const shared_ptr<TupleSet> &tupleSet) {
  // evaluate the expression to get input of aggregation
  const auto &expAggChunkedArray = evaluateExpr(tupleSet);
  if (!expAggChunkedArray.has_value()) {
    return tl::make_unexpected(expAggChunkedArray.error());
  }

  // compute the aggregation
  const auto &aggChunkdArray = *expAggChunkedArray;
  if (aggChunkdArray->length() > 1) {
    return tl::make_unexpected("More than one row returned 'One' aggregate function, please check the input query.");
  }
  if (aggChunkdArray->length() == 0) {
    // the caller should handle this case
    return nullptr;
  }
  auto expScalar = aggChunkdArray->GetScalar(0);
  if (!expScalar.ok()) {
    return tl::make_unexpected(expScalar.status().message());
  }
  return *expScalar;
}

tl::expected<shared_ptr<AggregateResult>, string> One::computePartial(const shared_ptr<TupleSet> &tupleSet) {
  // compute the result scalar
  const auto &expResultScalar = computeComplete(tupleSet);
  if (!expResultScalar) {
    return tl::make_unexpected(expResultScalar.error());
  }

  // make the aggregateResult
  auto aggregateResult = make_shared<AggregateResult>();
  aggregateResult->put(ONE_RESULT_KEY, *expResultScalar);
  return aggregateResult;
}

tl::expected<shared_ptr<arrow::Scalar>, string>
One::finalize(const vector<shared_ptr<AggregateResult>> &aggregateResults) {
  // build aggregate input array
  const auto expFinalizeInputArray = buildFinalizeInputArray(aggregateResults, ONE_RESULT_KEY, returnType());
  if (!expFinalizeInputArray) {
    return tl::make_unexpected(expFinalizeInputArray.error());
  }

  // compute the final aggregation, it's guaranteed that `aggregateResults` is not empty
  const auto &finalizeInputArray = *expFinalizeInputArray;
  if (finalizeInputArray ->length() > 1) {
    return tl::make_unexpected("More than one row returned 'One' aggregate function, please check the input query.");
  }
  auto expScalar = finalizeInputArray->GetScalar(0);
  if (!expScalar.ok()) {
    return tl::make_unexpected(expScalar.status().message());
  }
  return *expScalar;
}

std::vector<std::tuple<arrow::compute::internal::Aggregate, arrow::FieldRef, std::string,
std::shared_ptr<arrow::Field>>> One::getArrowAggregateSignatures() {
  static auto defaultScalarAggregateOptions = arrow::compute::ScalarAggregateOptions::Defaults();
  std::tuple<arrow::compute::internal::Aggregate, arrow::FieldRef, std::string, std::shared_ptr<arrow::Field>>
          aggregateSignature{
          {"hash_one", &defaultScalarAggregateOptions},
          getAggregateInputColumnName(),
          outputColumnName_,
          arrow::field(outputColumnName_, returnType())
  };
  return {aggregateSignature};
}

}
