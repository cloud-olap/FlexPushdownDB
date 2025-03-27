//
// Created by matt on 11/12/19.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_AGGREGATE_AGGREGATEPOP_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_AGGREGATE_AGGREGATEPOP_H

#include <fpdb/executor/physical/aggregate/function/AggregateFunction.h>
#include <fpdb/executor/physical/aggregate/AggregateResult.h>
#include <fpdb/executor/physical/PhysicalOp.h>
#include <fpdb/executor/message/TupleSetMessage.h>
#include <fpdb/executor/message/CompleteMessage.h>
#include <memory>
#include <string>
#include <vector>

using namespace fpdb::executor::message;

namespace fpdb::executor::physical::aggregate {

class AggregatePOp : public fpdb::executor::physical::PhysicalOp {

public:
  AggregatePOp(string name,
               vector<string> projectColumnNames,
               int nodeId,
               vector<shared_ptr<AggregateFunction>> functions,
               bool isReduce);
  AggregatePOp() = default;
  AggregatePOp(const AggregatePOp&) = default;
  AggregatePOp& operator=(const AggregatePOp&) = default;
  ~AggregatePOp() override = default;

  void onReceive(const Envelope &message) override;
  void clear() override;
  std::string getTypeString() const override;

  const vector<shared_ptr<AggregateFunction>> &getFunctions() const;
  bool isReduce() const;

private:
  void onStart();
  void onTupleSet(const TupleSetMessage &message);
  void onComplete(const CompleteMessage &message);

  void compute(const shared_ptr<TupleSet> &tupleSet);
  shared_ptr<TupleSet> finalize();
  shared_ptr<TupleSet> finalizeEmpty();

  bool hasResult();

  /**
   * Used in hybrid execution, keep only those aggregate functions that are applicable to input tupleSet
   * @param tupleSet
   */
  void discardInapplicableFunctions(const std::shared_ptr<TupleSet> &tupleSet);
  
  vector<shared_ptr<AggregateFunction>> functions_;
  vector<vector<shared_ptr<AggregateResult>>> aggregateResults_;

  // Whether this aggregate is to produce the finalized result
  bool isReduce_;

  /**
   * Whether discardInapplicableFunctions() has been invoked
   */
  bool inapplicableFunctionsDiscarded_ = false;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, AggregatePOp& op) {
    return inspect_base(f, op,
                        f.field("functions", op.functions_),
                        f.field("aggregateResults", op.aggregateResults_),
                        f.field("isReduce", op.isReduce_));
  }
};

}

#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_AGGREGATE_AGGREGATEPOP_H
