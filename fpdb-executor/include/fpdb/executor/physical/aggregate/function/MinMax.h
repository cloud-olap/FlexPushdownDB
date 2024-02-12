//
// Created by Yifei Yang on 12/14/21.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_AGGREGATE_FUNCTION_MINMAX_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_AGGREGATE_FUNCTION_MINMAX_H

#include <fpdb/executor/physical/aggregate/function/AggregateFunction.h>

namespace fpdb::executor::physical::aggregate {

/**
 * As arrow computes min and max in the same api together, we here also make them the same class
 */
class MinMax : public AggregateFunction {

public:
  MinMax(bool isMin,
         const string &outputColumnName,
         const shared_ptr<fpdb::expression::gandiva::Expression> &expression);
  MinMax() = default;
  MinMax(const MinMax&) = default;
  MinMax& operator=(const MinMax&) = default;

  std::string getTypeString() const override;
  ::nlohmann::json toJson() const override;

  tl::expected<shared_ptr<arrow::Scalar>, string> computeComplete(const shared_ptr<TupleSet> &tupleSet) override;
  tl::expected<shared_ptr<AggregateResult>, string> computePartial(const shared_ptr<TupleSet> &tupleSet) override;

  tl::expected<shared_ptr<arrow::Scalar>, string>
  finalize(const vector<shared_ptr<AggregateResult>> &aggregateResults) override;

  std::vector<std::tuple<arrow::compute::internal::Aggregate, arrow::FieldRef, std::string,
  std::shared_ptr<arrow::Field>>> getArrowAggregateSignatures() override;

private:
  constexpr static const char *const MIN_RESULT_KEY = "MIN";
  constexpr static const char *const MAX_RESULT_KEY = "MAX";

  bool isMin_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, MinMax& func) {
    return f.object(func).fields(f.field("type", func.type_),
                                 f.field("outputColumnName", func.outputColumnName_),
                                 f.field("expression", func.expression_),
                                 f.field("isMin", func.isMin_));
  }
};

}


#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_AGGREGATE_FUNCTION_MINMAX_H
