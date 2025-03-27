//
// Created by Yifei Yang on 12/10/24.
//

#include <fpdb/executor/physical/aggregate/function/Stddev.h>
#include <fpdb/executor/physical/aggregate/function/AggregateFunctionType.h>
#include <arrow/compute/api_aggregate.h>
#include <arrow/compute/cast.h>

namespace fpdb::executor::physical::aggregate {

Stddev::Stddev(StddevType stddevType,
               const string &outputColumnName,
               const shared_ptr<fpdb::expression::gandiva::Expression> &expression) :
  StddevBase(stddevType, STDDEV, outputColumnName, expression) {}

std::string Stddev::getTypeString() const {
  return "Stddev";
}

tl::expected<shared_ptr<arrow::Scalar>, string>
Stddev::computeComplete(const shared_ptr<TupleSet> &) {
  return tl::make_unexpected("Compute complete for Stddev is unimplemented");
}

tl::expected<shared_ptr<AggregateResult>, string>
Stddev::computePartial(const shared_ptr<TupleSet> &) {
  return tl::make_unexpected("Compute partial for Stddev is unimplemented");
}

std::vector<std::tuple<arrow::compute::internal::Aggregate, arrow::FieldRef, std::string,
std::shared_ptr<arrow::Field>>> Stddev::getArrowAggregateSignatures() {
  throw std::runtime_error("Get Arrow aggregate signatures for Stddev is unimplemented");
}

}
