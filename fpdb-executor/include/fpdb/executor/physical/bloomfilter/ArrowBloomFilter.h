//
// Created by Yifei Yang on 11/23/22.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_BLOOMFILTER_ARROWBLOOMFILTER_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_BLOOMFILTER_ARROWBLOOMFILTER_H

#include <fpdb/executor/physical/bloomfilter/BloomFilterBase.h>
#include <fpdb/tuple/arrow/exec/BloomFilter.h>
#include <fpdb/tuple/RecordBatchHasher.h>
#include <fpdb/tuple/TupleSet.h>

using namespace fpdb::tuple;

namespace fpdb::executor::physical::bloomfilter {

class ArrowBloomFilter: public BloomFilterBase {

public:
  ArrowBloomFilter(int64_t capacity, const std::vector<std::string> &columnNames);

  static std::shared_ptr<ArrowBloomFilter> make(int64_t capacity, const std::vector<std::string> &columnNames);

  ArrowBloomFilter() = default;
  ArrowBloomFilter(const ArrowBloomFilter&) = default;
  ArrowBloomFilter& operator=(const ArrowBloomFilter&) = default;
  ~ArrowBloomFilter() override = default;

  const std::shared_ptr<arrow::compute::BlockedBloomFilter> &getBlockedBloomFilter() const;
  void setBlockedBloomFilter(const std::shared_ptr<arrow::compute::BlockedBloomFilter> &blockedBloomFilter);

  tl::expected<void, std::string> build(const std::shared_ptr<TupleSet> &tupleSet);

  tl::expected<void, std::string> saveBitmapRecordBatches(const arrow::RecordBatchVector &batches) override;
  tl::expected<arrow::RecordBatchVector, std::string> makeBitmapRecordBatches() const override;
  ::nlohmann::json toJson() const override;
  static tl::expected<std::shared_ptr<ArrowBloomFilter>, std::string> fromJson(const nlohmann::json &jObj);

private:
  static constexpr std::string_view SER_MASKS_COLUMN = "masks";
  static constexpr std::string_view SER_BITMAP_COLUMN = "bitmap";

  std::vector<std::string> columnNames_;

  std::shared_ptr<arrow::compute::BlockedBloomFilter> blockedBloomFilter_;
  std::shared_ptr<RecordBatchHasher> hasher_;

// caf inspect (currently bloom filter is never sent across compute nodes, so this is never called)
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, ArrowBloomFilter& bf) {
    return f.object(bf).fields(f.field("capacity", bf.capacity_),
                               f.field("columnNames", bf.columnNames_));
  }
};

}


#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_BLOOMFILTER_ARROWBLOOMFILTER_H
