//
// Created by Yifei Yang on 11/23/22.
//

#include <fpdb/executor/physical/bloomfilter/ArrowBloomFilter.h>
#include <fpdb/executor/physical/Globals.h>
#include <fpdb/tuple/serialization/ArrowSerializer.h>
#include <fpdb/tuple/RecordBatchHasher.h>

namespace fpdb::executor::physical::bloomfilter {

ArrowBloomFilter::ArrowBloomFilter(int64_t capacity, const std::vector<std::string> &columnNames):
  BloomFilterBase(BloomFilterType::ARROW_BF, capacity, true),
  columnNames_(columnNames) {}

std::shared_ptr<ArrowBloomFilter> ArrowBloomFilter::make(int64_t capacity, 
                                                         const std::vector<std::string> &columnNames) {
  return std::make_shared<ArrowBloomFilter>(capacity, columnNames);
}

const std::shared_ptr<arrow::compute::BlockedBloomFilter> &ArrowBloomFilter::getBlockedBloomFilter() const {
  return blockedBloomFilter_;
}

void ArrowBloomFilter::setBlockedBloomFilter(
        const std::shared_ptr<arrow::compute::BlockedBloomFilter> &blockedBloomFilter) {
  blockedBloomFilter_ = blockedBloomFilter;
}

tl::expected<void, std::string> ArrowBloomFilter::build(const std::shared_ptr<TupleSet> &tupleSet) {
  // make hasher
  auto expHasher = RecordBatchHasher::make(tupleSet->schema(), columnNames_);
  if (!expHasher.has_value()) {
    return tl::make_unexpected(expHasher.error());
  }
  
  // init bloom filter
  using namespace arrow::compute;
  blockedBloomFilter_ = std::make_shared<BlockedBloomFilter>();
  auto hardwareFlags = arrow::internal::CpuInfo::GetInstance()->hardware_flags();
  auto builder = BloomFilterBuilder::Make(BloomFilterBuildStrategy::SINGLE_THREADED);
  auto res = builder->Begin(1, hardwareFlags, arrow::default_memory_pool(), tupleSet->numRows(), 0, 
                            1, blockedBloomFilter_.get());
  if (!res.ok()) {
    return tl::make_unexpected(res.message());
  }
  
  // push batches into bloom filter
  arrow::TableBatchReader reader{*tupleSet->table()};
  auto expRecordBatch = reader.Next();
  if (!expRecordBatch.ok()) {
    return tl::make_unexpected(expRecordBatch.status().message());
  }
  auto recordBatch = *expRecordBatch;
  while (recordBatch) {
    // use 32/64-bit hashes according to the initialized bloom filter
    if (blockedBloomFilter_->use_64bit_hashes()) {
      uint64_t *hashes = (uint64_t*) malloc(sizeof(uint64_t) * recordBatch->num_rows());
      (*expHasher)->hash(recordBatch, hashes);
      res = builder->PushNextBatch(0, recordBatch->num_rows(), hashes);
      free(hashes);
      if (!res.ok()) {
        return tl::make_unexpected(res.message());
      }
    } else {
      uint32_t *hashes = (uint32_t*) malloc(sizeof(uint32_t) * recordBatch->num_rows());
      (*expHasher)->hash(recordBatch, hashes);
      res = builder->PushNextBatch(0, recordBatch->num_rows(), hashes);
      free(hashes);
      if (!res.ok()) {
        return tl::make_unexpected(res.message());
      }
    }

    // next batch
    expRecordBatch = reader.Next();
    if (!expRecordBatch.ok()) {
      return tl::make_unexpected(expRecordBatch.status().message());
    }
    recordBatch = *expRecordBatch;
  }

  return {};
}

tl::expected<void, std::string>
ArrowBloomFilter::saveBitmapRecordBatches(const arrow::RecordBatchVector &batches) {
  return blockedBloomFilter_->saveBitmapRecordBatches(batches);
}

tl::expected<arrow::RecordBatchVector, std::string> ArrowBloomFilter::makeBitmapRecordBatches() const {
  return blockedBloomFilter_->makeBitmapRecordBatches();
}

::nlohmann::json ArrowBloomFilter::toJson() const {
  ::nlohmann::json jObj;
  jObj.emplace("type", type_);
  jObj.emplace("capacity", capacity_);
  jObj.emplace("columnNames", columnNames_);
  jObj.emplace("blockedBloomFilter", blockedBloomFilter_->toJson());
  return jObj;
}

tl::expected<std::shared_ptr<ArrowBloomFilter>, std::string> ArrowBloomFilter::fromJson(const nlohmann::json &jObj) {
  if (!jObj.contains("capacity")) {
    return tl::make_unexpected(fmt::format("Capacity not specified in ArrowBloomFilter JSON '{}'", to_string(jObj)));
  }
  auto capacity = jObj["capacity"].get<int64_t>();

  if (!jObj.contains("columnNames")) {
    return tl::make_unexpected(fmt::format("ColumnNames not specified in ArrowBloomFilter JSON '{}'", to_string(jObj)));
  }
  auto columnNames = jObj["columnNames"].get<std::vector<std::string>>();
  auto arrowBloomFilter = make(capacity, columnNames);

  if (!jObj.contains("blockedBloomFilter")) {
    return tl::make_unexpected(fmt::format("BlockedBloomFilter not specified in ArrowBloomFilter JSON '{}'", to_string(jObj)));
  }
  auto expBlockedBloomFilter = arrow::compute::BlockedBloomFilter::fromJson(jObj["blockedBloomFilter"]);
  if (!expBlockedBloomFilter.has_value()) {
    return tl::make_unexpected(expBlockedBloomFilter.error());
  }
  arrowBloomFilter->setBlockedBloomFilter(*expBlockedBloomFilter);

  return arrowBloomFilter;
}

}
