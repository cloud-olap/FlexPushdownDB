//
// Created by Yifei Yang on 12/9/24.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_AGGREGATE_FUNCTION_ONE_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_AGGREGATE_FUNCTION_ONE_H

#include <fpdb/executor/physical/aggregate/function/AggregateFunction.h>

namespace fpdb::executor::physical::aggregate {

class One : public AggregateFunction {
public:
  One(const string &outputColumnName,
      const shared_ptr<fpdb::expression::gandiva::Expression> &expression);
  One() = default;
  One(const One&) = default;
  One& operator=(const One&) = default;

  std::string getTypeString() const override;

  tl::expected<shared_ptr<arrow::Scalar>, string> computeComplete(const shared_ptr<TupleSet> &tupleSet) override;
  tl::expected<shared_ptr<AggregateResult>, string> computePartial(const shared_ptr<TupleSet> &tupleSet) override;

  tl::expected<shared_ptr<arrow::Scalar>, string>
  finalize(const vector<shared_ptr<AggregateResult>> &aggregateResults) override;

  std::vector<std::tuple<arrow::compute::internal::Aggregate, arrow::FieldRef, std::string,
  std::shared_ptr<arrow::Field>>> getArrowAggregateSignatures() override;

private:
  constexpr static const char *const ONE_RESULT_KEY = "ONE";

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, One& func) {
    return f.object(func).fields(f.field("type", func.type_),
                                 f.field("outputColumnName", func.outputColumnName_),
                                 f.field("expression", func.expression_));
  }
};

}

#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_AGGREGATE_FUNCTION_ONE_H
