//
// Created by Yifei Yang on 11/23/22.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_BLOOMFILTER_BLOOMFILTERBASE_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_BLOOMFILTER_BLOOMFILTERBASE_H

#include <arrow/api.h>
#include <nlohmann/json.hpp>
#include <tl/expected.hpp>

namespace fpdb::executor::physical::bloomfilter {

enum BloomFilterType {
  BLOOM_FILTER,
  ARROW_BLOOM_FILTER
};

class BloomFilterBase {

public:
  BloomFilterBase(BloomFilterType type, int64_t capacity, bool valid);
  BloomFilterBase() = default;
  BloomFilterBase(const BloomFilterBase&) = default;
  BloomFilterBase& operator=(const BloomFilterBase&) = default;
  virtual ~BloomFilterBase() = default;

  BloomFilterType getType() const;
  bool valid() const;

  virtual tl::expected<void, std::string> saveBitmapRecordBatches(const arrow::RecordBatchVector &batches) = 0;
  virtual tl::expected<arrow::RecordBatchVector, std::string> makeBitmapRecordBatches() const = 0;
  virtual ::nlohmann::json toJson() const = 0;
  static tl::expected<std::shared_ptr<BloomFilterBase>, std::string> fromJson(const nlohmann::json &jObj);

protected:
  BloomFilterType type_;
  int64_t capacity_;
  bool valid_;    // whether this bloom filter will be used after runtime checking (i.e. false if input is too large)
};

}


#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_BLOOMFILTER_BLOOMFILTERBASE_H
