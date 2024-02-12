//
// Created by Yifei Yang on 12/13/21.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_SPLIT_SPLITPOP_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_SPLIT_SPLITPOP_H

#include <fpdb/executor/physical/PhysicalOp.h>
#include <fpdb/executor/message/CompleteMessage.h>
#include <fpdb/executor/message/TupleSetMessage.h>
#include <fpdb/tuple/TupleSet.h>

using namespace fpdb::executor::message;
using namespace fpdb::tuple;
using namespace std;

namespace fpdb::executor::physical::split {

class SplitPOp : public PhysicalOp {

public:
  SplitPOp(const string &name,
           const vector<string> &projectColumnNames,
           int nodeId);
  SplitPOp() = default;
  SplitPOp(const SplitPOp&) = default;
  SplitPOp& operator=(const SplitPOp&) = default;

  void onReceive(const Envelope &msg) override;
  std::string getTypeString() const override;
  void clear() override;
  void produce(const shared_ptr<PhysicalOp> &op) override;

private:
  void onStart();
  void onComplete(const CompleteMessage &);
  void onTupleSet(const TupleSetMessage &message);

  tl::expected<void, string> splitAndSend();
  tl::expected<void, string> bufferInput(const shared_ptr<TupleSet>& tupleSet);
  tl::expected<vector<shared_ptr<TupleSet>>, string> split();
  void send(const vector<shared_ptr<TupleSet>> &tupleSets);

  vector<string> consumerVec_;
  std::optional<shared_ptr<TupleSet>> inputTupleSet_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, SplitPOp& op) {
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
                               f.field("consumerVec", op.consumerVec_));
  }
};

}


#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_SPLIT_SPLITPOP_H
