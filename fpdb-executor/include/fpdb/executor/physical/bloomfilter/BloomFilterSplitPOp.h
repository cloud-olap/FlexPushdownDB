//
// Created by Yifei Yang on 5/9/24.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_BLOOMFILTER_BLOOMFILTERSPLITPOP_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_BLOOMFILTER_BLOOMFILTERSPLITPOP_H

#include <fpdb/executor/physical/PhysicalOp.h>
#include <fpdb/executor/message/BloomFilterMessage.h>

namespace fpdb::executor::physical::bloomfilter {

/**
 * Split a dist global bf to multiple parts and send one part to one node
 */
class BloomFilterSplitPOp: public PhysicalOp {
public:
  BloomFilterSplitPOp(const std::string &name,
                      const std::vector<std::string> &projectColumnNames,
                      int nodeId);
  BloomFilterSplitPOp() = default;
  BloomFilterSplitPOp(const BloomFilterSplitPOp&) = default;
  BloomFilterSplitPOp& operator=(const BloomFilterSplitPOp&) = default;
  ~BloomFilterSplitPOp() = default;

  void onReceive(const Envelope &envelope) override;
  void clear() override;
  std::string getTypeString() const override;
  void produce(const std::shared_ptr<PhysicalOp> &op) override;

private:
  void onStart();
  void onBloomFilter(const BloomFilterMessage &msg);
  void onComplete(const CompleteMessage &);
  
  std::vector<std::string> consumerVec_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, BloomFilterSplitPOp& op) {
    return inspect_base(f, op,
                        f.field("consumerVec", op.consumerVec_));
  }
};

}

#endif // FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_BLOOMFILTER_BLOOMFILTERSPLITPOP_H
