//
// Created by Yifei Yang on 10/23/23.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_BLOOMFILTER_GLOBALARROWBLOOMFILTER_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_BLOOMFILTER_GLOBALARROWBLOOMFILTER_H

#include <fpdb/executor/physical/bloomfilter/BloomFilterBase.h>
#include <fpdb/tuple/arrow/exec/BloomFilter.h>
#include <fpdb/tuple/RecordBatchHasher.h>
#include <fpdb/tuple/TupleSet.h>
#include <mutex>
#include <condition_variable>

namespace fpdb::executor::physical::bloomfilter {

/**
 * Global Arrow Bloom filter built by multiple actors in parallel.
 */
class GlobalArrowBloomFilter: public BloomFilterBase {
public:
  GlobalArrowBloomFilter(int64_t capacity,
                         const std::vector<std::string> &columnNames,
                         int numShared,
                         int numDistSubBf);
  GlobalArrowBloomFilter() = default;
  GlobalArrowBloomFilter(const GlobalArrowBloomFilter&);
  GlobalArrowBloomFilter& operator=(const GlobalArrowBloomFilter&);
  ~GlobalArrowBloomFilter() override = default;

  const std::shared_ptr<arrow::compute::BlockedBloomFilter> getBlockedBloomFilter() const;

  tl::expected<void, std::string> saveBitmapRecordBatches(const arrow::RecordBatchVector &batches) override;
  tl::expected<arrow::RecordBatchVector, std::string> makeBitmapRecordBatches() const override;
  ::nlohmann::json toJson() const override;

  // create bf builder and blocked bf
  tl::expected<void, std::string> init(const std::shared_ptr<arrow::compute::BloomFilterMasks> &masks);

  // return if build is complete globally
  tl::expected<void, std::string> build(const std::shared_ptr<tuple::RecordBatchHasher> &hasher,
                                        const std::shared_ptr<tuple::TupleSet> &tupleSet,
                                        int threadId);

  // merge another BF into this
  tl::expected<void, std::string> merge(const std::shared_ptr<GlobalArrowBloomFilter> &other);
  tl::expected<void, std::string> mergePart(const std::shared_ptr<GlobalArrowBloomFilter> &other,
                                            int64_t blockOffset, int64_t numNlocks);

  // split BF into multiple parts
  tl::expected<std::vector<std::shared_ptr<GlobalArrowBloomFilter>>, std::string> split(uint n) const;

  // concat BF parts into a complete BF
  static tl::expected<std::shared_ptr<GlobalArrowBloomFilter>, std::string>
  concat(const std::vector<std::shared_ptr<GlobalArrowBloomFilter>> &parts);

private:
  // parallel insertion limits batch size by "PartitionSort::Eval()", see "PartitionUtil.h"
  static constexpr int64_t MaxBatchSize_ = 1 << 15;

  // input parameters
  std::vector<std::string> columnNames_;
  int numShared_;      // how many actors/threads share this state (e.g. parallel degree)
  int numDistSubBf_;   // if in dist exec, how many sub-bf will be created, i.e. how many nodes will create their own
                       // sub-bf, default is 1, and in probing, one row passes as long as it passes one sub-bf

  // runtime state
  std::shared_ptr<arrow::compute::BlockedBloomFilter> blockedBloomFilter_;
  std::unique_ptr<arrow::compute::BloomFilterBuilder> builder_;

// caf inspect (when sent across compute nodes, bitmap of "blockedBloomFilter_" is transferred by another Flight call)
// "numShared_" and "numDistSubBf_" are not needed since they are only used when created by "GlobalBloomFilterInitPOp"
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, GlobalArrowBloomFilter& bf) {
    return f.object(bf).fields(f.field("type", bf.type_),
                               f.field("capacity", bf.capacity_),
                               f.field("valid", bf.valid_),
                               f.field("columnNames", bf.columnNames_),
                               f.field("blockedBloomFilter", bf.blockedBloomFilter_));
  }
};

}

#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_BLOOMFILTER_GLOBALARROWBLOOMFILTER_H
