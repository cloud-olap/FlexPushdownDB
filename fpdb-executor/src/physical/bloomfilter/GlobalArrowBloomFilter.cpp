//
// Created by Yifei Yang on 10/23/23.
//

#include <fpdb/executor/physical/bloomfilter/GlobalArrowBloomFilter.h>
#include <fpdb/executor/physical/Globals.h>

namespace fpdb::executor::physical::bloomfilter {

GlobalArrowBloomFilter::GlobalArrowBloomFilter(int64_t capacity,
                                               const std::vector<std::string> &columnNames,
                                               int numShared,
                                               int numDistSubBf):
  BloomFilterBase(BloomFilterType::GLOBAL_ARROW_BF, capacity, true),
  columnNames_(columnNames),
  numShared_(numShared),
  numDistSubBf_(numDistSubBf) {}

GlobalArrowBloomFilter::GlobalArrowBloomFilter(const GlobalArrowBloomFilter& other):
  BloomFilterBase(other),
  columnNames_(other.columnNames_),
  numShared_(other.numShared_),
  numDistSubBf_(other.numDistSubBf_),
  blockedBloomFilter_(other.blockedBloomFilter_) {}

GlobalArrowBloomFilter& GlobalArrowBloomFilter::operator=(const GlobalArrowBloomFilter& other) {
  reinterpret_cast<BloomFilterBase&>(*this) = other;
  columnNames_ = other.columnNames_;
  numShared_ = other.numShared_;
  numDistSubBf_ = other.numDistSubBf_;
  blockedBloomFilter_ = other.blockedBloomFilter_;
  return *this;
}

const std::shared_ptr<arrow::compute::BlockedBloomFilter> GlobalArrowBloomFilter::getBlockedBloomFilter() const {
  return blockedBloomFilter_;
}

tl::expected<void, std::string>
GlobalArrowBloomFilter::saveBitmapRecordBatches(const arrow::RecordBatchVector &batches) {
  return blockedBloomFilter_->saveBitmapRecordBatches(batches);
}

tl::expected<arrow::RecordBatchVector, std::string>
GlobalArrowBloomFilter::makeBitmapRecordBatches() const {
  return blockedBloomFilter_->makeBitmapRecordBatches();
}

::nlohmann::json GlobalArrowBloomFilter::toJson() const {
  // unused
  return ::nlohmann::json();
}

tl::expected<void, std::string> GlobalArrowBloomFilter::init(
        const std::shared_ptr<arrow::compute::BloomFilterMasks> &masks) {
  // init blocked bloom filter
  blockedBloomFilter_ = std::make_shared<arrow::compute::BlockedBloomFilter>();
  auto hardwareFlags = arrow::internal::CpuInfo::GetInstance()->hardware_flags();
  builder_ = arrow::compute::BloomFilterBuilder::Make(arrow::compute::BloomFilterBuildStrategy::PARALLEL);
  auto res = builder_->Begin(numShared_, hardwareFlags, arrow::default_memory_pool(), capacity_, 0 /*unused*/,
                             numDistSubBf_, blockedBloomFilter_.get());
  if (!res.ok()) {
    return tl::make_unexpected(res.message());
  }
  if (masks != nullptr) {
    blockedBloomFilter_->setMasks(masks);
  }
  return {};
}

tl::expected<void, std::string> GlobalArrowBloomFilter::build(const std::shared_ptr<tuple::RecordBatchHasher> &hasher,
                                                              const std::shared_ptr<tuple::TupleSet> &tupleSet,
                                                              int threadId) {
  // push batches into bloom filter
  arrow::TableBatchReader reader{*tupleSet->table()};
  reader.set_chunksize(MaxBatchSize_);
  auto expRecordBatch = reader.Next();
  if (!expRecordBatch.ok()) {
    return tl::make_unexpected(expRecordBatch.status().message());
  }
  auto recordBatch = *expRecordBatch;
  while (recordBatch) {
    // Arrow BF builder does not allow empty input batch
    if (recordBatch->num_rows() > 0) {
      // use 32/64-bit hashes according to the initialized bloom filter
      if (blockedBloomFilter_->use_64bit_hashes()) {
        uint64_t *hashes = (uint64_t *) malloc(sizeof(uint64_t) * recordBatch->num_rows());
        hasher->hash(recordBatch, hashes);
        auto res = builder_->PushNextBatch(threadId, recordBatch->num_rows(), hashes);
        free(hashes);
        if (!res.ok()) {
          return tl::make_unexpected(res.message());
        }
      } else {
        uint32_t *hashes = (uint32_t *) malloc(sizeof(uint32_t) * recordBatch->num_rows());
        hasher->hash(recordBatch, hashes);
        auto res = builder_->PushNextBatch(threadId, recordBatch->num_rows(), hashes);
        free(hashes);
        if (!res.ok()) {
          return tl::make_unexpected(res.message());
        }
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
GlobalArrowBloomFilter::merge(const std::shared_ptr<GlobalArrowBloomFilter> &other) {
  return blockedBloomFilter_->merge(other->blockedBloomFilter_);
}

tl::expected<void, std::string>
GlobalArrowBloomFilter::mergePart(const std::shared_ptr<GlobalArrowBloomFilter> &other,
                                  int64_t blockOffset, int64_t numBlocks) {
  return blockedBloomFilter_->mergePart(other->blockedBloomFilter_, blockOffset, numBlocks);
}

tl::expected<std::vector<std::shared_ptr<GlobalArrowBloomFilter>>, std::string>
GlobalArrowBloomFilter::split(uint n) const {
  std::vector<std::shared_ptr<GlobalArrowBloomFilter>> splitRes{n};
  auto expBlockedBfSplitRes = blockedBloomFilter_->split(n);
  if (!expBlockedBfSplitRes.has_value()) {
    return tl::make_unexpected(expBlockedBfSplitRes.error());
  }
  auto blockedBfSplitRes = *expBlockedBfSplitRes;
  for (uint i = 0; i < n; ++i) {
    // only make a split for non-skipped entry
    if (blockedBfSplitRes[i] != nullptr) {
      splitRes[i] = std::make_shared<GlobalArrowBloomFilter>(
          capacity_, columnNames_, 1, numDistSubBf_ /*all params here are actually unused*/);
      splitRes[i]->blockedBloomFilter_ = blockedBfSplitRes[i]; // this is the only param needed
    }
  }
  return splitRes;
}

tl::expected<std::shared_ptr<GlobalArrowBloomFilter>, std::string>
GlobalArrowBloomFilter::concat(const std::vector<std::shared_ptr<GlobalArrowBloomFilter>> &parts) {
  // specical conditions
  if (parts.empty()) {
    return tl::make_unexpected("No parts when concatenating GlobalArrowBloomFilter parts");
  }
  // regular path
  std::vector<std::shared_ptr<arrow::compute::BlockedBloomFilter>> blockedBfParts{parts.size()};
  for (uint i = 0; i < parts.size(); ++i) {
    blockedBfParts[i] = parts[i]->blockedBloomFilter_;
  }
  auto concatedBlockedBf = arrow::compute::BlockedBloomFilter::concat(blockedBfParts);
  if (!concatedBlockedBf.has_value()) {
    return tl::make_unexpected(concatedBlockedBf.error());
  }
  auto concated = std::make_shared<GlobalArrowBloomFilter>(
      parts[0]->capacity_, parts[0]->columnNames_, 1, parts[0]->numDistSubBf_ /*all params here are actually unused*/);
  concated->blockedBloomFilter_ = *concatedBlockedBf;     // this is the only param needed
  return concated;
}

}
