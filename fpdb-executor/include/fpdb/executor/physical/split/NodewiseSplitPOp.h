//
// Created by Yifei Yang on 3/16/24.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_SPLIT_NODEWISESPLITPOP_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_SPLIT_NODEWISESPLITPOP_H

#include <fpdb/executor/physical/PhysicalOp.h>

namespace fpdb::executor::physical::split {

class NodewiseSplitPOp : public PhysicalOp {
  
public:
  NodewiseSplitPOp(const std::string &name,
                   const std::vector<std::string> &projectColumnNames,
                   int nodeId,
                   int numNodes);
  NodewiseSplitPOp() = default;
  NodewiseSplitPOp(const NodewiseSplitPOp&) = default;
  NodewiseSplitPOp& operator=(const NodewiseSplitPOp&) = default;

  void onReceive(const Envelope &msg) override;
  std::string getTypeString() const override;
  void clear() override;

  using PhysicalOp::produce;    // tell the compiler we want this func of both base and derived class
  void produce(const std::shared_ptr<PhysicalOp> &op, int group);

private:
  void onStart();
  void onComplete(const CompleteMessage &);
  void onTupleSet(const TupleSetMessage &message);

  tl::expected<void, string> splitAndSend(int group);
  tl::expected<void, string> bufferInput(const shared_ptr<TupleSet>& tupleSet, int group);
  void send(const std::vector<shared_ptr<TupleSet>> &tupleSets, int group);
  void initInputBuffers();

  int numNodes_;
  std::vector<std::vector<std::string>> consumerGroups_;  // consumers grouped by incoming nodes that they are responsible for
  std::vector<std::shared_ptr<TupleSet>> inputGroups_;    // input tables grouped by incoming nodes
  std::vector<bool> sentResultGroups_;                    // whether result has been sent grouped by incoming nodes

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, NodewiseSplitPOp& op) {
    return inspect_base(f, op,
                        f.field("numNodes", op.numNodes_),
                        f.field("consumerGroups", op.consumerGroups_));
  }
};

}

#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_SPLIT_NODEWISESPLITPOP_H
