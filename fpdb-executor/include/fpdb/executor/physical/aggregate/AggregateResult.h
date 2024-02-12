//
// Created by matt on 7/3/20.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_AGGREGATE_AGGREGATERESULT_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_AGGREGATE_AGGREGATERESULT_H

#include <fpdb/tuple/TupleSet.h>
#include <fpdb/tuple/Scalar.h>
#include <tl/expected.hpp>
#include <string>
#include <unordered_map>

using namespace std;

namespace fpdb::executor::physical::aggregate {

/**
 * Structure for aggregate functions to store intermediate results.
 *
 * It is intended for there to be one of these per aggregate function.
 *
 * This is roughly equivalent to a map, so aggregate functions can store multiple values, before
 * computing the final result. E.g. Mean most accurately calculated by storing count and sum before computing
 * the final result.
 */
class AggregateResult {

public:
  AggregateResult() = default;
  AggregateResult(const AggregateResult&) = default;
  AggregateResult& operator=(const AggregateResult&) = default;

  void put(const string &key, const shared_ptr<arrow::Scalar> &value);
  tl::expected<shared_ptr<arrow::Scalar>, string> get(const string &key);

private:
  // use fpdb::tuple::Scalar instead of arrow::Scalar for ease of serialization
  unordered_map<string, shared_ptr<fpdb::tuple::Scalar>> resultMap_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, AggregateResult& res) {
    return f.apply(res.resultMap_);
  }
};

}

using AggregateResultPtr = std::shared_ptr<fpdb::executor::physical::aggregate::AggregateResult>;

CAF_BEGIN_TYPE_ID_BLOCK(AggregateResult, fpdb::caf::CAFUtil::AggregateResult_first_custom_type_id)
CAF_ADD_TYPE_ID(AggregateResult, (fpdb::executor::physical::aggregate::AggregateResult))
CAF_END_TYPE_ID_BLOCK(AggregateResult)

namespace caf {
template <>
struct inspector_access<AggregateResultPtr> : variant_inspector_access<AggregateResultPtr> {
  // nop
};
} // namespace caf

#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_AGGREGATE_AGGREGATERESULT_H
