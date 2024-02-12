//
// Created by Yifei Yang on 1/26/22.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_AGGREGATE_FUNCTION_AVGBASE_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_AGGREGATE_FUNCTION_AVGBASE_H

#include <fpdb/executor/physical/aggregate/function/AggregateFunction.h>

namespace fpdb::executor::physical::aggregate {

/**
 * Abstract base class derived by Avg and AvgReduce
 */
class AvgBase: public AggregateFunction {

public:
  AvgBase(AggregateFunctionType type,
          const string &outputColumnName,
          const shared_ptr<fpdb::expression::gandiva::Expression> &expression);
  AvgBase() = default;
  AvgBase(const AvgBase&) = default;
  AvgBase& operator=(const AvgBase&) = default;

  shared_ptr<arrow::DataType> returnType() const override;
  static shared_ptr<arrow::DataType> defaultReturnType();

  tl::expected<shared_ptr<arrow::Scalar>, string>
  finalize(const vector<shared_ptr<AggregateResult>> &aggregateResults) override;

  tl::expected<shared_ptr<arrow::ChunkedArray>, std::string> finalize(const shared_ptr<TupleSet> &tupleSet);

  tl::expected<pair<shared_ptr<arrow::Field>, shared_ptr<arrow::ChunkedArray>>, std::string>
  getIntermediateSumColumn(const shared_ptr<TupleSet> &tupleSet) const;
  tl::expected<pair<shared_ptr<arrow::Field>, shared_ptr<arrow::ChunkedArray>>, std::string>
  getIntermediateCountColumn(const shared_ptr<TupleSet> &tupleSet) const;

protected:
  constexpr static const char *const SUM_RESULT_KEY = "SUM";
  constexpr static const char *const COUNT_RESULT_KEY = "COUNT";

  std::string getIntermediateSumColumnName() const;
  std::string getIntermediateCountColumnName() const;

};

}


#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_AGGREGATE_FUNCTION_AVGBASE_H
