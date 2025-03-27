//
// Created by Yifei Yang on 5/9/24.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_BLOOMFILTER_BLOOMFILTERCONCATPOP_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_BLOOMFILTER_BLOOMFILTERCONCATPOP_H

#include <fpdb/executor/physical/PhysicalOp.h>
#include <fpdb/executor/message/BloomFilterMessage.h>

namespace fpdb::executor::physical::bloomfilter {

/**
 * Concatenate parts of a dist global bf from all the other nodes
 */
class BloomFilterConcatPOp: public PhysicalOp {
public:
  BloomFilterConcatPOp(const std::string &name,
                       const std::vector<std::string> &projectColumnNames,
                       int nodeId);
  BloomFilterConcatPOp() = default;
  BloomFilterConcatPOp(const BloomFilterConcatPOp&) = default;
  BloomFilterConcatPOp& operator=(const BloomFilterConcatPOp&) = default;
  ~BloomFilterConcatPOp() = default;

  void consume(const std::shared_ptr<PhysicalOp> &op) override;
  void onReceive(const Envelope &envelope) override;
  void clear() override;
  std::string getTypeString() const override;
  
private:
  void onStart();
  void onBloomFilter(const BloomFilterMessage &msg);
  void onComplete(const CompleteMessage &);

  std::unordered_map<std::string, uint> producerIds_;   // keep an order
  
  // runtime states
  std::vector<std::shared_ptr<BloomFilterBase>> bloomFilterParts_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, BloomFilterConcatPOp& op) {
    return inspect_base(f, op,
                        f.field("producerIds", op.producerIds_));
  }
};

}

#endif // FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_BLOOMFILTER_BLOOMFILTERCONCATPOP_H
