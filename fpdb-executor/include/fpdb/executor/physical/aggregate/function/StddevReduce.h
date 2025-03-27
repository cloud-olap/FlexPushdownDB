//
// Created by Yifei Yang on 12/10/24.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_AGGREGATE_FUNCTION_STDDEVREDUCE_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_AGGREGATE_FUNCTION_STDDEVREDUCE_H

#include <fpdb/executor/physical/aggregate/function/StddevBase.h>

namespace fpdb::executor::physical::aggregate {

/**
 * Stddev reduce function, the difference from Stddev is that sum, count, and sum of squares values are
 * already computed from producers
 */
class StddevReduce : public StddevBase {
public:
  StddevReduce(StddevType stddevType,
               const string &outputColumnName,
               const shared_ptr<fpdb::expression::gandiva::Expression> &expression);
  StddevReduce() = default;
  StddevReduce(const StddevReduce&) = default;
  StddevReduce& operator=(const StddevReduce&) = default;

  std::string getTypeString() const override;
  set<string> involvedColumnNames() const override;

  tl::expected<shared_ptr<arrow::Scalar>, string> computeComplete(const shared_ptr<TupleSet> &tupleSet) override;
  tl::expected<shared_ptr<AggregateResult>, string> computePartial(const shared_ptr<TupleSet> &tupleSet) override;

  std::vector<std::tuple<arrow::compute::internal::Aggregate, arrow::FieldRef, std::string,
  std::shared_ptr<arrow::Field>>> getArrowAggregateSignatures() override;

  // only StddevReduce needs to set this explicitly
  void setAggColumnDataType(const std::shared_ptr<TupleSet> &tupleSet);

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, StddevReduce& func) {
    return f.object(func).fields(f.field("stddevType", func.stddevType_),
                                 f.field("type", func.type_),
                                 f.field("outputColumnName", func.outputColumnName_),
                                 f.field("expression", func.expression_));
  }
};

}

#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_AGGREGATE_FUNCTION_STDDEVREDUCE_H
