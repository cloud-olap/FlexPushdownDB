//
// Created by Yifei Yang on 12/10/24.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_AGGREGATE_FUNCTION_STDDEVBASE_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_AGGREGATE_FUNCTION_STDDEVBASE_H

#include <fpdb/executor/physical/aggregate/function/AggregateFunction.h>

namespace fpdb::executor::physical::aggregate {

enum StddevType {
    SAMP,
    POP
};

/**
 * Abstract base class derived by Stddev and StddevReduce
 */
class StddevBase: public AggregateFunction {
public:
  StddevBase(StddevType stddevType,
             AggregateFunctionType funcType,
             const string &outputColumnName,
             const shared_ptr<fpdb::expression::gandiva::Expression> &expression);
  StddevBase() = default;
  StddevBase(const StddevBase&) = default;
  StddevBase& operator=(const StddevBase&) = default;

  shared_ptr<arrow::DataType> returnType() const override;
  static shared_ptr<arrow::DataType> defaultReturnType();

  tl::expected<shared_ptr<arrow::Scalar>, string>
  finalize(const vector<shared_ptr<AggregateResult>> &aggregateResults) override;

  tl::expected<shared_ptr<arrow::ChunkedArray>, std::string> finalize(const shared_ptr<TupleSet> &tupleSet);

  tl::expected<pair<shared_ptr<arrow::Field>, shared_ptr<arrow::ChunkedArray>>, std::string>
  getIntermediateSumColumn(const shared_ptr<TupleSet> &tupleSet) const;
  tl::expected<pair<shared_ptr<arrow::Field>, shared_ptr<arrow::ChunkedArray>>, std::string>
  getIntermediateCountColumn(const shared_ptr<TupleSet> &tupleSet) const;
  tl::expected<pair<shared_ptr<arrow::Field>, shared_ptr<arrow::ChunkedArray>>, std::string>
  getIntermediateSumOfSquaresColumn(const shared_ptr<TupleSet> &tupleSet) const;

  StddevType getStddevType() const;

protected:
  constexpr static const char *const SUM_RESULT_KEY = "SUM";
  constexpr static const char *const COUNT_RESULT_KEY = "COUNT";
  constexpr static const char *const SUM_OF_SQUARES_RESULT_KEY = "SUM_OF_SQUARES";

  std::string getIntermediateSumColumnName() const;
  std::string getIntermediateCountColumnName() const;
  std::string getIntermediateSumOfSquaresColumnName() const;

  StddevType stddevType_;
};

}

#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_AGGREGATE_FUNCTION_STDDEVBASE_H
