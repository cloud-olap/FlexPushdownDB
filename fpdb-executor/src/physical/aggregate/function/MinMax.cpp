//
// Created by Yifei Yang on 12/14/21.
//

#include <fpdb/executor/physical/aggregate/function/MinMax.h>
#include <fpdb/executor/physical/aggregate/function/AggregateFunctionType.h>
#include <arrow/compute/api_aggregate.h>

namespace fpdb::executor::physical::aggregate {

MinMax::MinMax(bool isMin,
               const string &outputColumnName,
               const shared_ptr<fpdb::expression::gandiva::Expression> &expression):
  AggregateFunction(MIN_MAX, outputColumnName, expression),
  isMin_(isMin) {}

std::string MinMax::getTypeString() const {
  return "MinMax";
}

::nlohmann::json MinMax::toJson() const {
  ::nlohmann::json jObj;
  auto typeString = isMin_ ? "Min" : "Max";
  jObj.emplace("type", typeString);
  jObj.emplace("outputColumnName", outputColumnName_);
  jObj.emplace("expression", expression_->toJson());
  return jObj;
}

tl::expected<shared_ptr<arrow::Scalar>, string> MinMax::computeComplete(const shared_ptr<TupleSet> &tupleSet) {
  // evaluate the expression to get input of aggregation
  const auto &expAggChunkedArray = evaluateExpr(tupleSet);
  if (!expAggChunkedArray.has_value()) {
    return tl::make_unexpected(expAggChunkedArray.error());
  }

  // compute the aggregation
  const auto &expResultDatum = arrow::compute::MinMax(*expAggChunkedArray);
  if (!expResultDatum.ok()) {
    return tl::make_unexpected(expResultDatum.status().message());
  }
  const auto &resultScalar = (*expResultDatum).scalar();
  const auto &structScalar = static_pointer_cast<arrow::StructScalar>(resultScalar);
  return isMin_ ? structScalar->value[0] : structScalar->value[1];
}

tl::expected<shared_ptr<AggregateResult>, string> MinMax::computePartial(const shared_ptr<TupleSet> &tupleSet) {
  // compute the result scalar
  const auto &expResultScalar = computeComplete(tupleSet);
  if (!expResultScalar) {
    return tl::make_unexpected(expResultScalar.error());
  }

  // make the aggregateResult
  auto aggregateResult = make_shared<AggregateResult>();
  auto key = isMin_ ? MIN_RESULT_KEY : MAX_RESULT_KEY;
  aggregateResult->put(key, *expResultScalar);
  return aggregateResult;
}

tl::expected<shared_ptr<arrow::Scalar>, string>
MinMax::finalize(const vector<shared_ptr<AggregateResult>> &aggregateResults) {
  // build aggregate input array
  auto key = isMin_ ? MIN_RESULT_KEY : MAX_RESULT_KEY;
  const auto expFinalizeInputArray = buildFinalizeInputArray(aggregateResults, key, returnType());
  if (!expFinalizeInputArray) {
    return tl::make_unexpected(expFinalizeInputArray.error());
  }

  // compute the final aggregation
  const auto &expFinalResultScalar = arrow::compute::MinMax(expFinalizeInputArray.value());
  if (!expFinalResultScalar.ok()) {
    return tl::make_unexpected(expFinalResultScalar.status().message());
  }
  const auto &resultScalar = (*expFinalResultScalar).scalar();
  const auto &structScalar = static_pointer_cast<arrow::StructScalar>(resultScalar);
  const shared_ptr<arrow::Scalar> minMaxScalar = isMin_ ? structScalar->value[0] : structScalar->value[1];
  return minMaxScalar;
}

std::vector<std::tuple<arrow::compute::internal::Aggregate, arrow::FieldRef, std::string,
std::shared_ptr<arrow::Field>>> MinMax::getArrowAggregateSignatures() {
  static auto defaultScalarAggregateOptions = arrow::compute::ScalarAggregateOptions::Defaults();
  std::string arrowFunctionName = isMin_ ? "hash_min" : "hash_max";
  std::tuple<arrow::compute::internal::Aggregate, arrow::FieldRef, std::string, std::shared_ptr<arrow::Field>>
          aggregateSignature{
          {arrowFunctionName, &defaultScalarAggregateOptions},
          getAggregateInputColumnName(),
          outputColumnName_,
          arrow::field(outputColumnName_, returnType())
  };
  return {aggregateSignature};
}

}
