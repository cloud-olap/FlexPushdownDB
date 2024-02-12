//
// Created by matt on 29/4/20.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_HASHJOIN_HASHJOINBUILDPOP_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_HASHJOIN_HASHJOINBUILDPOP_H

#include <fpdb/executor/physical/join/hashjoin/HashJoinBuildKernel.h>
#include <fpdb/executor/physical/PhysicalOp.h>
#include <fpdb/executor/message/TupleSetMessage.h>
#include <fpdb/executor/message/CompleteMessage.h>
#include <arrow/scalar.h>
#include <unordered_map>

using namespace fpdb::executor::message;
using namespace std;

namespace fpdb::executor::physical::join {

/**
 * Operator implementing build phase of a hash join
 *
 * Builds a hash table of tuples from one of the relations in a join (ideally the smaller relation). That hashtable is
 * then used in the probe phase to select rows to add to the final joined relation.
 *
 */
class HashJoinBuildPOp : public PhysicalOp {

public:
  explicit HashJoinBuildPOp(const string &name,
                            const vector<string> &projectColumnNames,
                            int nodeId,
                            const vector<string> &predColumnNames);
  HashJoinBuildPOp() = default;
  HashJoinBuildPOp(const HashJoinBuildPOp&) = default;
  HashJoinBuildPOp& operator=(const HashJoinBuildPOp&) = default;

  void onReceive(const Envelope &msg) override;
  void clear() override;
  std::string getTypeString() const override;

private:
  void onStart();
  void onTupleSet(const TupleSetMessage &msg);
  void onComplete(const CompleteMessage &msg);

  tl::expected<void, string> buffer(const shared_ptr<TupleSet>& tupleSet);
  void send(bool force);

  HashJoinBuildKernel kernel_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, HashJoinBuildPOp& op) {
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
                               f.field("kernel", op.kernel_));
  }
};

}

#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_HASHJOIN_HASHJOINBUILDPOP_H
