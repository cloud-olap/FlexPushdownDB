//
// Created by Yifei Yang on 12/12/21.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_NESTEDLOOPJOIN_NESTEDLOOPJOINPOP_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_NESTEDLOOPJOIN_NESTEDLOOPJOINPOP_H

#include <fpdb/executor/physical/PhysicalOp.h>
#include <fpdb/executor/physical/join/nestedloopjoin/NestedLoopJoinKernel.h>
#include <fpdb/executor/message/TupleSetMessage.h>
#include <fpdb/executor/message/CompleteMessage.h>
#include <fpdb/executor/message/TupleSetIndexMessage.h>
#include <fpdb/plan/prephysical/JoinType.h>

using namespace fpdb::executor::message;
using namespace fpdb::plan::prephysical;
using namespace fpdb::expression::gandiva;
using namespace std;

namespace fpdb::executor::physical::join {

class NestedLoopJoinPOp : public PhysicalOp {
public:
  NestedLoopJoinPOp(const string &name,
                    const vector<string> &projectColumnNames,
                    int nodeId,
                    const std::optional<shared_ptr<expression::gandiva::Expression>> &predicate,
                    JoinType joinType);
  NestedLoopJoinPOp() = default;
  NestedLoopJoinPOp(const NestedLoopJoinPOp&) = default;
  NestedLoopJoinPOp& operator=(const NestedLoopJoinPOp&) = default;

  void onReceive(const Envelope &msg) override;
  void clear() override;
  std::string getTypeString() const override;

  void addLeftProducer(const shared_ptr<PhysicalOp> &leftProducer);
  void addRightProducer(const shared_ptr<PhysicalOp> &rightProducer);

private:
  void onStart();
  void onComplete(const CompleteMessage &);
  void onTupleSet(const TupleSetMessage &message);

  NestedLoopJoinKernel makeKernel(const std::optional<shared_ptr<expression::gandiva::Expression>> &predicate,
                                  JoinType joinType);
  void send(bool force);
  void sendEmpty();

  set<string> leftProducers_;
  set<string> rightProducers_;

  NestedLoopJoinKernel kernel_;
  bool sentResult = false;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, NestedLoopJoinPOp& op) {
    return f.object(op).fields(f.field("name", op.name_),
                               f.field("type", op.type_),
                               f.field("projectColumnNames", op.projectColumnNames_),
                               f.field("nodeId", op.nodeId_),
                               f.field("queryId", op.queryId_),
                               f.field("opContext", op.opContext_),
                               f.field("producers", op.producers_),
                               f.field("consumers", op.consumers_),
                               f.field("consumerToBloomFilterInfo", op.consumerToBloomFilterInfo_),
                               f.field("isSeparated", op.isSeparated_),
                               f.field("leftProducers", op.leftProducers_),
                               f.field("rightProducers", op.rightProducers_),
                               f.field("kernel", op.kernel_));
  }
};

}


#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_NESTEDLOOPJOIN_NESTEDLOOPJOINPOP_H
