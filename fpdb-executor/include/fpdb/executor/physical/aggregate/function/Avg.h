//
// Created by Yifei Yang on 1/25/22.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_AGGREGATE_FUNCTION_AVG_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_AGGREGATE_FUNCTION_AVG_H

#include <fpdb/executor/physical/aggregate/function/AvgBase.h>

namespace fpdb::executor::physical::aggregate {
  
class Avg : public AvgBase {
  
public:
  Avg(const string &outputColumnName,
      const shared_ptr<fpdb::expression::gandiva::Expression> &expression);
  Avg() = default;
  Avg(const Avg&) = default;
  Avg& operator=(const Avg&) = default;

  std::string getTypeString() const override;

  tl::expected<shared_ptr<arrow::Scalar>, string> computeComplete(const shared_ptr<TupleSet> &tupleSet) override;
  tl::expected<shared_ptr<AggregateResult>, string> computePartial(const shared_ptr<TupleSet> &tupleSet) override;

  std::vector<std::tuple<arrow::compute::internal::Aggregate, arrow::FieldRef, std::string,
  std::shared_ptr<arrow::Field>>> getArrowAggregateSignatures() override;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, Avg& func) {
    return f.object(func).fields(f.field("type", func.type_),
                                 f.field("outputColumnName", func.outputColumnName_),
                                 f.field("expression", func.expression_));
  }
};

}


#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_AGGREGATE_FUNCTION_AVG_H
