//
// Created by matt on 7/3/20.
//

#include <fpdb/executor/physical/aggregate/AggregateResult.h>

using namespace fpdb::executor::physical::aggregate;
using namespace fpdb::tuple;

void AggregateResult::put(const string &key, const shared_ptr<arrow::Scalar> &value) {
  this->resultMap_.insert_or_assign(key, Scalar::make(value));
}

tl::expected<shared_ptr<arrow::Scalar>, string> AggregateResult::get(const string &key) {
  auto resIt = this->resultMap_.find(key);
  if (resIt == this->resultMap_.end()) {
    return tl::make_unexpected(fmt::format("Aggregate result key not found: {}", key));
  } else {
    return resIt->second->getArrowScalar();
  }
}
