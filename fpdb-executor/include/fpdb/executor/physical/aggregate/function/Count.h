//
// Created by Yifei Yang on 12/3/21.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_AGGREGATE_FUNCTION_COUNT_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_AGGREGATE_FUNCTION_COUNT_H

#include <fpdb/executor/physical/aggregate/function/AggregateFunction.h>

namespace fpdb::executor::physical::aggregate {

class Count : public AggregateFunction {

public:
  Count(const string &outputColumnName,
        const shared_ptr<fpdb::expression::gandiva::Expression> &expression);
  Count() = default;
  Count(const Count&) = default;
  Count& operator=(const Count&) = default;

  std::string getTypeString() const override;
  shared_ptr<arrow::DataType> returnType() const override;
  static shared_ptr<arrow::DataType> defaultReturnType();
  set<string> involvedColumnNames() const override;

  tl::expected<shared_ptr<arrow::Scalar>, string> computeComplete(const shared_ptr<TupleSet> &tupleSet) override;
  tl::expected<shared_ptr<AggregateResult>, string> computePartial(const shared_ptr<TupleSet> &tupleSet) override;

  tl::expected<shared_ptr<arrow::Scalar>, string>
  finalize(const vector<shared_ptr<AggregateResult>> &aggregateResults) override;

  std::vector<std::tuple<arrow::compute::internal::Aggregate, arrow::FieldRef, std::string,
  std::shared_ptr<arrow::Field>>> getArrowAggregateSignatures() override;

private:
  constexpr static const char *const COUNT_RESULT_KEY = "COUNT";

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, Count& func) {
    return f.object(func).fields(f.field("type", func.type_),
                                 f.field("outputColumnName", func.outputColumnName_),
                                 f.field("expression", func.expression_));
  }
};

}


#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_AGGREGATE_FUNCTION_COUNT_H
