//
// Created by Yifei Yang on 11/23/22.
//

#include <fpdb/executor/physical/bloomfilter/ArrowBloomFilter.h>
#include <fpdb/executor/physical/Globals.h>
#include <fpdb/tuple/serialization/ArrowSerializer.h>

namespace fpdb::executor::physical::bloomfilter {

ArrowBloomFilter::ArrowBloomFilter(int64_t capacity, const std::vector<std::string> &columnNames):
  BloomFilterBase(BloomFilterType::ARROW_BLOOM_FILTER, capacity, capacity <= BLOOM_FILTER_MAX_INPUT_SIZE),
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
  hasher_ = *expHasher;
  
  // init bloom filter
  using namespace arrow::compute;
  blockedBloomFilter_ = std::make_shared<BlockedBloomFilter>();
  auto hardwareFlags = arrow::internal::CpuInfo::GetInstance()->hardware_flags();
  auto builder = BloomFilterBuilder::Make(BloomFilterBuildStrategy::SINGLE_THREADED);
  auto res = builder->Begin(1, hardwareFlags, arrow::default_memory_pool(), tupleSet->numRows(), 0, 
                            blockedBloomFilter_.get());
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
    uint32_t* hashes = (uint32_t*) malloc(sizeof(uint32_t) * recordBatch->num_rows());
    hasher_->hash(recordBatch, hashes);
    res = builder->PushNextBatch(0, recordBatch->num_rows(), hashes);
    if (!res.ok()) {
      // clear
      free(hashes);
      return tl::make_unexpected(res.message());
    }

    // clear
    free(hashes);
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
  // should contain exactly two batches
  if (batches.size() != 2) {
    return tl::make_unexpected("RecordBatch stream for ArrowBloomFilter's bitmap should contain two recordBatches");
  }
  const auto &masksBatch = batches[0];
  const auto &bitmapBatch = batches[1];

  // masks
  const auto &masksBuffer = masksBatch->column(0)->data()->buffers[1];
  blockedBloomFilter_->SetPrivateMasks(
          std::make_shared<arrow::compute::BloomFilterMasks>(masksBuffer->data(), masksBuffer->size()));

  // bitmap
  blockedBloomFilter_->SetBuf(bitmapBatch->column(0)->data()->buffers[1]);

  return {};
}

tl::expected<arrow::RecordBatchVector, std::string> ArrowBloomFilter::makeBitmapRecordBatches() const {
  // schema: one uint_8 column
  auto schema = arrow::schema({{field(ArrowSerializer::BITMAP_FIELD_NAME.data(), arrow::uint8())}});

  // masks batch
  const auto &masks = arrow::compute::BlockedBloomFilter::GetGlobalMasks();
  int numBytes = arrow::compute::BloomFilterMasks::kTotalBytes;
  auto buffer = std::make_shared<arrow::Buffer>(masks.masks_, numBytes);
  auto masksArray = std::make_shared<arrow::NumericArray<arrow::UInt8Type>>(
          arrow::ArrayData::Make(arrow::uint8(), numBytes, {nullptr, buffer}));
  auto masksBatch = arrow::RecordBatch::Make(schema, numBytes, {masksArray});

  // bitmap array
  numBytes = sizeof(uint64_t) * blockedBloomFilter_->num_blocks();
  auto bitmapArray = std::make_shared<arrow::NumericArray<arrow::UInt8Type>>(arrow::ArrayData::Make(
          arrow::uint8(), numBytes, {nullptr, blockedBloomFilter_->buf()}));
  auto bitmapBatch = arrow::RecordBatch::Make(schema, numBytes, {bitmapArray});

  return arrow::RecordBatchVector{masksBatch, bitmapBatch};
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
