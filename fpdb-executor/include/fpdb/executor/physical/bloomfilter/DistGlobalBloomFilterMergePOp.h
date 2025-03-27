//
// Created by Yifei Yang on 4/16/24.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_BLOOMFILTER_DISTGLOBALBLOOMFILTERMERGEPOP_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_BLOOMFILTER_DISTGLOBALBLOOMFILTERMERGEPOP_H

#include <fpdb/executor/physical/bloomfilter/GlobalBloomFilterFinalizePOp.h>

namespace fpdb::executor::physical::bloomfilter {

class DistGlobalBloomFilterMergePOp: public GlobalBloomFilterFinalizePOp {

public:
  DistGlobalBloomFilterMergePOp(const std::string &name,
                                const std::vector<std::string> &projectColumnNames,
                                int nodeId);
  DistGlobalBloomFilterMergePOp() = default;
  DistGlobalBloomFilterMergePOp(const DistGlobalBloomFilterMergePOp&) = default;
  DistGlobalBloomFilterMergePOp& operator=(const DistGlobalBloomFilterMergePOp&) = default;
  ~DistGlobalBloomFilterMergePOp() = default;

  std::string getTypeString() const override;

private:
  void onBloomFilter(const BloomFilterMessage &msg) override;
  tl::expected<void, std::string> merge(const std::shared_ptr<BloomFilterBase> &other);
  void mergePart(const std::shared_ptr<BloomFilterBase> &other, int64_t blockOffset, int64_t numBlocks, uint id);

  std::vector<std::string> errVec_;   // used in parallel merge

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, DistGlobalBloomFilterMergePOp& op) {
    return inspect_base(f, op,
                        f.field("localBloomFilterReceivers", op.localBloomFilterReceivers_),
                        f.field("remoteBloomFilterReceivers", op.remoteBloomFilterReceivers_));
  }
};

}

#endif // FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_BLOOMFILTER_DISTGLOBALBLOOMFILTERMERGEPOP_H
