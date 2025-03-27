//
// Created by Yifei Yang on 4/1/24.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_BLOOMFILTER_GLOBALBLOOMFILTERFINALIZEPOP_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_BLOOMFILTER_GLOBALBLOOMFILTERFINALIZEPOP_H

#include <fpdb/executor/physical/PhysicalOp.h>
#include <fpdb/executor/message/BloomFilterMessage.h>

namespace fpdb::executor::physical::bloomfilter {

/**
 * Sync parallel "BloomFilterCreatePOp" when building a global BF
 */
class GlobalBloomFilterFinalizePOp: public PhysicalOp {

public:
  GlobalBloomFilterFinalizePOp(const std::string &name,
                               const std::vector<std::string> &projectColumnNames,
                               int nodeId);
  GlobalBloomFilterFinalizePOp() = default;
  GlobalBloomFilterFinalizePOp(const GlobalBloomFilterFinalizePOp&) = default;
  GlobalBloomFilterFinalizePOp& operator=(const GlobalBloomFilterFinalizePOp&) = default;
  ~GlobalBloomFilterFinalizePOp() = default;

  static void connectToProducers(const std::shared_ptr<PhysicalOp> &bfFinalize,
                                 const std::shared_ptr<PhysicalOp> &bfInit,
                                 const std::vector<std::shared_ptr<PhysicalOp>> &bfCreateVec);
  static void connectToConsumers(const std::shared_ptr<PhysicalOp> &bfFinalize,
                                 const std::vector<std::shared_ptr<PhysicalOp>> &localBloomFilterReceivers,
                                 const std::vector<std::shared_ptr<PhysicalOp>> &remoteBloomFilterReceivers);

  void onReceive(const Envelope &envelope) override;
  void clear() override;
  std::string getTypeString() const override;

  void addLocalBloomFilterReceiver(const std::shared_ptr<PhysicalOp> &op);
  void addRemoteBloomFilterReceiver(const std::shared_ptr<PhysicalOp> &op);

private:
  void onStart();
  virtual void onBloomFilter(const BloomFilterMessage &msg);
  void onComplete(const CompleteMessage &);

protected:
  // FIXME: we should use opEntry.nodeId_ to check if consumers are local or remote (see "BloomFilterSplitPOp") instead
  std::set<std::string> localBloomFilterReceivers_;   // "BloomFilterUsePOp"s
  std::set<std::string> remoteBloomFilterReceivers_;  // used when broadcasting bloom filter to remote nodes
  std::shared_ptr<BloomFilterBase> bloomFilter_ = nullptr;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, GlobalBloomFilterFinalizePOp& op) {
    return inspect_base(f, op,
                        f.field("localBloomFilterReceivers", op.localBloomFilterReceivers_),
                        f.field("remoteBloomFilterReceivers", op.remoteBloomFilterReceivers_));
  }
};

}

#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_BLOOMFILTER_GLOBALBLOOMFILTERFINALIZEPOP_H
