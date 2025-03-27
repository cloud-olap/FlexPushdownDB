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

  void recordPredTransCard(const executor::cache::PredTransCardCache::PredTransCardKey &key);

private:
  void onStart();
  void onComplete(const CompleteMessage &);
  void onTupleSet(const TupleSetMessage &message);

  tl::expected<void, string> splitAndSend();
  tl::expected<void, string> bufferInput(const shared_ptr<TupleSet>& tupleSet);
  void send(const vector<shared_ptr<TupleSet>> &tupleSets);

  vector<string> consumerVec_;
  std::optional<shared_ptr<TupleSet>> inputTupleSet_;
  bool sentResult_ = false;

  // to record runtime cardinalities
  executor::cache::PredTransCardCache::PredTransCardInfo ptCardInfo_;
  int64_t numRows_ = 0;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, SplitPOp& op) {
    return inspect_base(f, op,
                        f.field("consumerVec", op.consumerVec_),
                        f.field("ptCardInfo", op.ptCardInfo_));
  }
};

}


#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_SPLIT_SPLITPOP_H
