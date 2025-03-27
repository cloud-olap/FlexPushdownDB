//
// Created by Yifei Yang on 12/10/24.
//

#include <fpdb/executor/physical/aggregate/function/StddevReduce.h>
#include <fpdb/executor/physical/aggregate/function/Count.h>
#include <arrow/compute/api_aggregate.h>
#include <arrow/compute/cast.h>

namespace fpdb::executor::physical::aggregate {

StddevReduce::StddevReduce(StddevType stddevType,
                           const string &outputColumnName,
                           const shared_ptr<fpdb::expression::gandiva::Expression> &expression) :
  StddevBase(stddevType, STDDEV_REDUCE, outputColumnName, expression) {}

std::string StddevReduce::getTypeString() const {
  return "StddevReduce";
}

set<string> StddevReduce::involvedColumnNames() const {
  return {getIntermediateSumColumnName(), getIntermediateCountColumnName(),
          getIntermediateSumOfSquaresColumnName()};
}

tl::expected<shared_ptr<arrow::Scalar>, string>
StddevReduce::computeComplete(const shared_ptr<TupleSet> &) {
  return tl::make_unexpected("Compute complete for StddevReduce is unimplemented");
}

tl::expected<shared_ptr<AggregateResult>, string>
StddevReduce::computePartial(const shared_ptr<TupleSet> &) {
  return tl::make_unexpected("Compute partial for StddevReduce is unimplemented");
}

std::vector<std::tuple<arrow::compute::internal::Aggregate, arrow::FieldRef, std::string,
std::shared_ptr<arrow::Field>>> StddevReduce::getArrowAggregateSignatures() {
  static auto defaultScalarAggregateOptions = arrow::compute::ScalarAggregateOptions::Defaults();
  auto intermediateSumColumnName = getIntermediateSumColumnName();
  auto intermediateCountColumnName = getIntermediateCountColumnName();
  auto intermediateSumOfSquaresColumnName = getIntermediateSumOfSquaresColumnName();

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
  std::tuple<arrow::compute::internal::Aggregate, arrow::FieldRef, std::string, std::shared_ptr<arrow::Field>>
          sumOfSquaresAggregateSignature{
          {"hash_sum", &defaultScalarAggregateOptions},
          intermediateSumOfSquaresColumnName,
          intermediateSumOfSquaresColumnName,
          arrow::field(intermediateSumOfSquaresColumnName, aggColumnDataType_)
  };
  return {sumAggregateSignature, countAggregateSignature, sumOfSquaresAggregateSignature};
}

void StddevReduce::setAggColumnDataType(const std::shared_ptr<TupleSet> &tupleSet) {
  if (aggColumnDataType_ == nullptr) {
    aggColumnDataType_ = tupleSet->schema()->GetFieldByName(getIntermediateSumColumnName())->type();
  }
}

}
