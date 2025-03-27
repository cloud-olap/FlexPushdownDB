//
// Created by Yifei Yang on 4/17/24.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_BLOOMFILTER_DISTGLOBALBLOOMFILTERINITPOP_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_BLOOMFILTER_DISTGLOBALBLOOMFILTERINITPOP_H

#include <fpdb/executor/physical/PhysicalOp.h>

namespace fpdb::executor::physical::bloomfilter {

/**
 * Initiate the build of a distributed global BF
 */
class DistGlobalBloomFilterInitPOp: public PhysicalOp {
public:
  DistGlobalBloomFilterInitPOp(const std::string &name,
                               const std::vector<std::string> &projectColumnNames,
                               int nodeId);
  DistGlobalBloomFilterInitPOp() = default;
  DistGlobalBloomFilterInitPOp(const DistGlobalBloomFilterInitPOp&) = default;
  DistGlobalBloomFilterInitPOp& operator=(const DistGlobalBloomFilterInitPOp&) = default;
  ~DistGlobalBloomFilterInitPOp() override = default;

  void onReceive(const Envelope &msg) override;
  std::string getTypeString() const override;
  void clear() override;

private:
  void onStart();
  void onTupleSetSize(const TupleSetSizeMessage &msg);
  void onComplete(const CompleteMessage &);

  int64_t numRows_ = 0;
  
// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, DistGlobalBloomFilterInitPOp& op) {
    return inspect_base(f, op);
  }
};

}

#endif // FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_BLOOMFILTER_DISTGLOBALBLOOMFILTERINITPOP_H
