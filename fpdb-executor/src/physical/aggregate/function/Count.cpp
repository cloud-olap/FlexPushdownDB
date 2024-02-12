//
// Created by Yifei Yang on 12/3/21.
//

#include <fpdb/executor/physical/aggregate/function/Count.h>
#include <fpdb/executor/physical/aggregate/function/AggregateFunctionType.h>
#include <arrow/compute/api_aggregate.h>
#include <fpdb/plan/prephysical/AggregatePrePFunction.h>

using namespace fpdb::plan::prephysical;

namespace fpdb::executor::physical::aggregate {

Count::Count(const string &outputColumnName,
             const shared_ptr<fpdb::expression::gandiva::Expression> &expression):
  AggregateFunction(COUNT, outputColumnName, expression) {}

std::string Count::getTypeString() const {
  return "Count";
}

shared_ptr<arrow::DataType> Count::returnType() const {
  return defaultReturnType();
}

shared_ptr<arrow::DataType> Count::defaultReturnType() {
  return arrow::int64();
}

set<string> Count::involvedColumnNames() const {
  if (expression_) {
    return expression_->involvedColumnNames();
  } else {
    return set<string>{AggregatePrePFunction::COUNT_STAR_COLUMN};
  }
}

tl::expected<shared_ptr<arrow::Scalar>, string> Count::computeComplete(const shared_ptr<TupleSet> &tupleSet) {
  if (expression_) {
    // if has expr, then evaluate it and call arrow api to count
    const auto &expAggChunkedArray = evaluateExpr(tupleSet);
    if (!expAggChunkedArray.has_value()) {
      return tl::make_unexpected(expAggChunkedArray.error());
    }
    const auto &expResultDatum = arrow::compute::Count(*expAggChunkedArray);
    if (!expResultDatum.ok()) {
      return tl::make_unexpected(expResultDatum.status().message());
    }
    return (*expResultDatum).scalar();
  }

  else {
    // otherwise, it means count(*), so we should just return the number of rows
    const auto &expResultScalar = arrow::MakeScalar(returnType(), tupleSet->numRows());
    if (!expResultScalar.ok()) {
      return tl::make_unexpected(expResultScalar.status().message());
    }
    return *expResultScalar;
  }
}

tl::expected<shared_ptr<AggregateResult>, string> Count::computePartial(const shared_ptr<TupleSet> &tupleSet) {
  // compute the result scalar
  const auto &expResultScalar = computeComplete(tupleSet);
  if (!expResultScalar) {
    return tl::make_unexpected(expResultScalar.error());
  }

  // make the aggregateResult
  auto aggregateResult = make_shared<AggregateResult>();
  aggregateResult->put(COUNT_RESULT_KEY, *expResultScalar);
  return aggregateResult;
}

tl::expected<shared_ptr<arrow::Scalar>, string>
Count::finalize(const vector<shared_ptr<AggregateResult>> &aggregateResults) {
  // build aggregate input array
  const auto expFinalizeInputArray = buildFinalizeInputArray(aggregateResults, COUNT_RESULT_KEY, returnType());
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
std::shared_ptr<arrow::Field>>> Count::getArrowAggregateSignatures() {
  static auto countOnlyValidOptions = arrow::compute::CountOptions(arrow::compute::CountOptions::ONLY_VALID);
  static auto countAllOptions = arrow::compute::CountOptions(arrow::compute::CountOptions::ALL);
  auto& countOptions = (expression_ != nullptr) ? countOnlyValidOptions : countAllOptions;
  std::tuple<arrow::compute::internal::Aggregate, arrow::FieldRef, std::string, std::shared_ptr<arrow::Field>>
          aggregateSignature{
          {"hash_count", &countOptions},
          getAggregateInputColumnName(),
          outputColumnName_,
          arrow::field(outputColumnName_, returnType())
  };
  return {aggregateSignature};
}

}
