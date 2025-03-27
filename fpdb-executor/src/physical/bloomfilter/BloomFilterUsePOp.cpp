//
// Created by Yifei Yang on 3/16/22.
//

#include <fpdb/executor/physical/bloomfilter/BloomFilterUsePOp.h>
#include <fpdb/executor/physical/bloomfilter/BloomFilterUseKernel.h>
#include <fpdb/executor/physical/bloomfilter/BloomFilter.h>
#include <fpdb/executor/physical/bloomfilter/ArrowBloomFilter.h>
#include <fpdb/executor/physical/bloomfilter/GlobalArrowBloomFilter.h>
#include <fpdb/util/Util.h>
#include <arrow/compute/api_vector.h>

namespace fpdb::executor::physical::bloomfilter {

BloomFilterUsePOp::BloomFilterUsePOp(const std::string &name,
                                     const std::vector<std::string> &projectColumnNames,
                                     int nodeId,
                                     const std::vector<std::string> &bloomFilterColumnNames,
                                     int numExpBloomFilters):
  PhysicalOp(name, BLOOM_FILTER_USE, projectColumnNames, nodeId),
  bloomFilterColumnNames_(bloomFilterColumnNames),
  numExpBloomFilters_(numExpBloomFilters) {}

std::string BloomFilterUsePOp::getTypeString() const {
  return "BloomFilterUsePOp";
}

void BloomFilterUsePOp::onReceive(const Envelope &envelope) {
  const auto &msg = envelope.message();

  if (msg.type() == MessageType::START) {
    this->onStart();
  } else if (msg.type() == MessageType::TUPLESET) {
    auto tupleSetMessage = dynamic_cast<const TupleSetMessage &>(msg);
    this->onTupleSet(tupleSetMessage);
  } else if (msg.type() == MessageType::BLOOM_FILTER) {
    auto bloomFilterMessage = dynamic_cast<const BloomFilterMessage &>(msg);
    this->onBloomFilter(bloomFilterMessage);
  } else if (msg.type() == MessageType::COMPLETE) {
    auto completeMessage = dynamic_cast<const CompleteMessage &>(msg);
    this->onComplete(completeMessage);
  } else {
    ctx()->notifyError("Unrecognized message type " + msg.getTypeString());
  }
}

const std::vector<std::string> &BloomFilterUsePOp::getBloomFilterColumnNames() const {
  return bloomFilterColumnNames_;
}

const std::shared_ptr<BloomFilterBase> &BloomFilterUsePOp::getStandAloneBloomFilter() const {
  return standAloneState_.bloomFilter_;
}

void BloomFilterUsePOp::setStandAloneBloomFilter(const std::shared_ptr<BloomFilterBase> &bloomFilter) {
  standAloneState_.bloomFilter_ = bloomFilter;
}

bool BloomFilterUsePOp::receivedStandAloneBloomFilter() const {
  return standAloneState_.bloomFilter_ != nullptr;
}

void BloomFilterUsePOp::clearProducersExceptBloomFilterCreate() {
  for (auto it = producers_.begin(); it != producers_.end(); ) {
    if (it->substr(0, 17) != "BloomFilterCreate") {
      it = producers_.erase(it);
    }
    else {
      ++it;
    }
  }
}

void BloomFilterUsePOp::recordPredTransCard(const executor::cache::PredTransCardCache::PredTransCardKey &key) {
  ptCardInfo_.collect_ = true;
  ptCardInfo_.key_ = key;
}

void BloomFilterUsePOp::onStart() {
  SPDLOG_DEBUG("Starting operator  |  name: '{}'", this->name());
}

void BloomFilterUsePOp::onTupleSet(const TupleSetMessage &msg) {
  // metrics
  auto tupleSet = msg.tuples();
#if SHOW_DEBUG_METRICS == true
  numRowsInput_ += tupleSet->numRows();
#endif

  numExpBloomFilters_ == 1 ? onTupleSetStandAlone(tupleSet) : onTupleSetDist(tupleSet);
}

void BloomFilterUsePOp::onTupleSetStandAlone(const std::shared_ptr<TupleSet> &tupleSet) {
  // Buffer tupleSet
  if (standAloneState_.tupleSet_ == nullptr) {
    standAloneState_.tupleSet_ = tupleSet;
  }
  else {
    auto expConcatenatedTupleSet = TupleSet::concatenate({standAloneState_.tupleSet_, tupleSet});
    if (!expConcatenatedTupleSet.has_value()) {
      ctx()->notifyError(expConcatenatedTupleSet.error());
      return;
    }
    standAloneState_.tupleSet_ = *expConcatenatedTupleSet;
  }

  // Filter and send
  if (standAloneState_.bloomFilter_ != nullptr) {
    auto result = filterAndSend();
    if (!result.has_value()) {
      ctx()->notifyError(result.error());
    }
  }
}

void BloomFilterUsePOp::onTupleSetDist(const std::shared_ptr<TupleSet> &tupleSet) {
  // buffer this table
  distState_.tupleSets_.emplace_back(tupleSet);

  // compute size of the output bitvector
  distState_.bitvecLen_.emplace_back(arrow::bit_util::BytesForBits(tupleSet->numRows()));

  // put a place hold for hashes, since we can only know use 32/64-bit after receiving bfs
  distState_.hashesVec32_.emplace_back(nullptr);
  distState_.hashesVec64_.emplace_back(nullptr);

  // filter this table using Bfs that have arrived
  auto res = filterOneTupleSet(distState_.tupleSets_.size() - 1);
  if (!res.has_value()) {
    ctx()->notifyError(res.error());
    return;
  }

  // if have received all expected bfs, produce output tables
  if ((int)distState_.bloomFilterVec_.size() == numExpBloomFilters_) {
    res = projectAndSend();
    if (!res.has_value()) {
      ctx()->notifyError(res.error());
      return;
    }
  }
}

void BloomFilterUsePOp::onBloomFilter(const BloomFilterMessage &msg) {
  auto bloomFilter = msg.getBloomFilter();
  numExpBloomFilters_ == 1 ? onBloomFilterStandAlone(bloomFilter) : onBloomFilterDist(bloomFilter);
}

void BloomFilterUsePOp::onBloomFilterStandAlone(const std::shared_ptr<BloomFilterBase> &bloomFilter) {
  // Buffer bloom filter
  standAloneState_.bloomFilter_ = bloomFilter;

  // Filter and send
  if (standAloneState_.tupleSet_ != nullptr) {
    auto result = filterAndSend();
    if (!result.has_value()) {
      ctx()->notifyError(result.error());
    }
  }
}

void BloomFilterUsePOp::onBloomFilterDist(const std::shared_ptr<BloomFilterBase> &bloomFilter) {
  // buffer this bf
  distState_.bloomFilterVec_.emplace_back(bloomFilter);

  // check valid
  if (!bloomFilter->valid()) {
    distState_.bloomFilterValid_ = false;
  }

  // filter all existing tupleSets on this bf
  auto res = filterOneBloomFilter(distState_.bloomFilterVec_.size() - 1);
  if (!res.has_value()) {
    ctx()->notifyError(res.error());
    return;
  }

  // if have received all expected bfs, produce output tables
  if ((int)distState_.bloomFilterVec_.size() == numExpBloomFilters_) {
    res = projectAndSend();
    if (!res.has_value()) {
      ctx()->notifyError(res.error());
      return;
    }
  }
}

void BloomFilterUsePOp::onComplete(const CompleteMessage &) {
  if (!ctx()->isComplete() && ctx()->operatorMap().allComplete(POpRelationshipType::Producer)) {
#if SHOW_DEBUG_METRICS == true
    // send hash join metrics
    if (executor::metrics::SHOW_BLOOM_FILTER_METRICS) {
      std::shared_ptr<Message> hjMetricsMsg = std::make_shared<HashJoinMetricsMessage>(
              executor::metrics::HashJoinMetrics(0, 0, 0, numRowsInput_), name_);
      ctx()->notifyRoot(hjMetricsMsg);
    }
    // send PT case study metrics
    if (ptCSMetricsInfo_.collPredTransCSMetrics_ && ptCSMetricsInfo_.collFiltering_) {
      metrics::PredTransCSMetrics::PTCSMetricsUnit ptCSMetricsUnit(ptCSMetricsInfo_);
      ptCSMetricsUnit.numRowsIn_ = numRowsInput_;
      ptCSMetricsUnit.numRowsOut_ = numRowsOutput_;
      std::shared_ptr<Message> ptCSMetricsMsg = std::make_shared<PredTransCSMetricsMessage>(
              ptCSMetricsUnit, name_);
      ctx()->notifyRoot(ptCSMetricsMsg);
    }
#endif

    // record cardinality if needed
    sendPTCardMessage(ptCardInfo_, numRowsOutput_, std::nullopt);

    ctx()->notifyComplete();
  }
}

tl::expected<void, std::string> BloomFilterUsePOp::filterAndSend() {
  std::shared_ptr<TupleSet> filteredTupleSet;
  
  if (standAloneState_.tupleSet_->numRows() == 0 || !standAloneState_.bloomFilter_->valid()) {
    // No filter for empty table and invalid bloom filter
    filteredTupleSet = standAloneState_.tupleSet_;
  } else {
    // Filter
    tl::expected<std::shared_ptr<TupleSet>, std::string> expFilteredTupleSet;
    switch (standAloneState_.bloomFilter_->getType()) {
      case BloomFilterType::VANILLA_BF: {
        // column indices
        if (columnIndices_ == nullptr) {
          auto expColumnIndices = BloomFilterUseKernel::makeColumnIndices(
                  standAloneState_.tupleSet_->schema(), bloomFilterColumnNames_);
          if (!expColumnIndices.has_value()) {
            return tl::make_unexpected(expColumnIndices.error());
          }
          columnIndices_ = *expColumnIndices;
        }
        // filter
        expFilteredTupleSet = BloomFilterUseKernel::filter(
                standAloneState_.tupleSet_,
                std::static_pointer_cast<BloomFilter>(standAloneState_.bloomFilter_),
                *columnIndices_);
        break;
      }
      case BloomFilterType::ARROW_BF:
      case BloomFilterType::GLOBAL_ARROW_BF: {
        // hasher
        auto res = makeHasher(standAloneState_.tupleSet_->schema());
        if (!res.has_value()) {
          return res;
        }
        // filter
        const auto &blockedBloomFilter = (standAloneState_.bloomFilter_->getType() == BloomFilterType::ARROW_BF) ?
                std::static_pointer_cast<ArrowBloomFilter>(standAloneState_.bloomFilter_)->getBlockedBloomFilter():
                std::static_pointer_cast<GlobalArrowBloomFilter>(standAloneState_.bloomFilter_)->getBlockedBloomFilter();
        expFilteredTupleSet = BloomFilterUseKernel::filter(standAloneState_.tupleSet_,
                                                           blockedBloomFilter,
                                                           hasher_);
        break;
      }
      default: {
        return tl::make_unexpected(
                fmt::format("Unknown bloom filter type: {}", standAloneState_.bloomFilter_->getType()));
      }
    }
    if (!expFilteredTupleSet.has_value()) {
      return tl::make_unexpected(expFilteredTupleSet.error());
    }
    filteredTupleSet = *expFilteredTupleSet;
  }

  // Send
  std::shared_ptr<Message> tupleSetMessage = std::make_shared<TupleSetMessage>(filteredTupleSet, name());
  ctx()->tell(tupleSetMessage);
  numRowsOutput_ += filteredTupleSet->numRows();

  // Clear buffer
  standAloneState_.tupleSet_.reset();

  return {};
}

tl::expected<void, std::string> BloomFilterUsePOp::makeHasher(const std::shared_ptr<arrow::Schema> &schema) {
  if (hasher_ == nullptr) {
    auto expHasher = RecordBatchHasher::make(schema, bloomFilterColumnNames_);
    if (!expHasher.has_value()) {
      return tl::make_unexpected(expHasher.error());
    }
    hasher_ = *expHasher;
  }
  return {};
}

tl::expected<uint32_t*, std::string> BloomFilterUsePOp::hash32(const std::shared_ptr<TupleSet> &tupleSet) {
  // make hasher
  auto res = makeHasher(tupleSet->schema());
  if (!res.has_value()) {
    return tl::make_unexpected(res.error());
  }

  // hash
  uint32_t* hashes = (uint32_t*) malloc(sizeof(uint32_t) * tupleSet->numRows());
  int64_t offset = 0;
  arrow::TableBatchReader reader{*tupleSet->table()};
  auto expRecordBatch = reader.Next();
  if (!expRecordBatch.ok()) {
    return tl::make_unexpected(expRecordBatch.status().message());
  }
  auto recordBatch = *expRecordBatch;
  while (recordBatch) {
    // hash batch
    hasher_->hash(recordBatch, hashes + offset);

    // next batch
    offset += recordBatch->num_rows();
    expRecordBatch = reader.Next();
    if (!expRecordBatch.ok()) {
      return tl::make_unexpected(expRecordBatch.status().message());
    }
    recordBatch = *expRecordBatch;
  }
  return hashes;
}


tl::expected<uint64_t*, std::string> BloomFilterUsePOp::hash64(const std::shared_ptr<TupleSet> &tupleSet) {
  // make hasher
  auto res = makeHasher(tupleSet->schema());
  if (!res.has_value()) {
    return tl::make_unexpected(res.error());
  }

  // hash
  uint64_t* hashes = (uint64_t*) malloc(sizeof(uint64_t) * tupleSet->numRows());
  int64_t offset = 0;
  arrow::TableBatchReader reader{*tupleSet->table()};
  auto expRecordBatch = reader.Next();
  if (!expRecordBatch.ok()) {
    return tl::make_unexpected(expRecordBatch.status().message());
  }
  auto recordBatch = *expRecordBatch;
  while (recordBatch) {
    // hash batch
    hasher_->hash(recordBatch, hashes + offset);

    // next batch
    offset += recordBatch->num_rows();
    expRecordBatch = reader.Next();
    if (!expRecordBatch.ok()) {
      return tl::make_unexpected(expRecordBatch.status().message());
    }
    recordBatch = *expRecordBatch;
  }
  return hashes;
}

tl::expected<void, std::string> BloomFilterUsePOp::filterOneTupleSet(uint tupleSetIdx) {
  // filter on all received bfs
  std::vector<uint8_t*> outVec_;
  for (uint bloomFilterIdx = 0; bloomFilterIdx < distState_.bloomFilterVec_.size(); ++bloomFilterIdx) {
    auto expOut = filter(tupleSetIdx, bloomFilterIdx);
    if (!expOut.has_value()) {
      return tl::make_unexpected(expOut.error());
    }
    outVec_.emplace_back(*expOut);
  }
  distState_.outs_.emplace_back(outVec_);
  return {};
}

tl::expected<void, std::string> BloomFilterUsePOp::filterOneBloomFilter(uint bloomFilterIdx) {
  for (uint tupleSetIdx = 0; tupleSetIdx < distState_.tupleSets_.size(); ++tupleSetIdx) {
    auto expOut = filter(tupleSetIdx, bloomFilterIdx);
    if (!expOut.has_value()) {
      return tl::make_unexpected(expOut.error());
    }
    distState_.outs_[tupleSetIdx].emplace_back(*expOut);
  }
  return {};
}

tl::expected<uint8_t*, std::string> BloomFilterUsePOp::filter(uint tupleSetIdx, uint bloomFilterIdx) {
  // get table at "tupleSetIdx" position
  const auto &tupleSet = distState_.tupleSets_[tupleSetIdx];
  int numRows = tupleSet->numRows();

  // get bf at "bloomFilterIdx" position
  const auto &bloomFilter = distState_.bloomFilterVec_[bloomFilterIdx];

  // currently only support using arrow blocked bloom filter
  auto type = bloomFilter->getType();
  if (type != BloomFilterType::ARROW_BF && type != BloomFilterType::GLOBAL_ARROW_BF) {
    return tl::make_unexpected(fmt::format("Unsupported bloom filter type to output as bitvector: {}", type));
  }
  const auto &blockedBloomFilter = (type == BloomFilterType::ARROW_BF) ?
          std::static_pointer_cast<ArrowBloomFilter>(bloomFilter)->getBlockedBloomFilter():
          std::static_pointer_cast<GlobalArrowBloomFilter>(bloomFilter)->getBlockedBloomFilter();

  // filter only when table is not empty and all bfs are valid
  if (numRows == 0 || !distState_.bloomFilterValid_) {
    return nullptr;
  }

  // use 32/64-bit hashes according to the bf
  uint8_t* out = (uint8_t*)malloc(distState_.bitvecLen_[tupleSetIdx]);
  if (blockedBloomFilter->use_64bit_hashes()) {
    // compute hashes if not already computed
    uint64_t* hashes = distState_.hashesVec64_[tupleSetIdx];
    if (hashes == nullptr) {
      auto expHashes = hash64(tupleSet);
      if (!expHashes.has_value()) {
        free(out);
        return tl::make_unexpected(expHashes.error());
      }
      hashes = *expHashes;
      distState_.hashesVec64_[tupleSetIdx] = hashes;
    }
    // filter
    blockedBloomFilter->Find(hasher_->getHardwareFlags(), numRows, hashes, out);
  } else {
    // compute hashes if not already computed
    uint32_t* hashes = distState_.hashesVec32_[tupleSetIdx];
    if (hashes == nullptr) {
      auto expHashes = hash32(tupleSet);
      if (!expHashes.has_value()) {
        free(out);
        return tl::make_unexpected(expHashes.error());
      }
      hashes = *expHashes;
      distState_.hashesVec32_[tupleSetIdx] = hashes;
    }
    // filter
    blockedBloomFilter->Find(hasher_->getHardwareFlags(), numRows, hashes, out);
  }
  return out;
}

tl::expected<void, std::string> BloomFilterUsePOp::projectAndSend() {
  for (uint i = 0; i < distState_.tupleSets_.size(); ++i) {
    const auto &tupleSet = distState_.tupleSets_[i];
    int64_t bitvecLen = distState_.bitvecLen_[i];
    auto &outVec = distState_.outs_[i];
    std::shared_ptr<TupleSet> filteredTupleSet;

    // no filtering if some bf is invalid
    if (tupleSet->numRows() == 0 || !distState_.bloomFilterValid_) {
      filteredTupleSet = tupleSet;
    } else {
      // bitvec "or"
      auto finalOut = bitvecOr(outVec, bitvecLen);
      // apply to the input table
      auto selectBuffer = std::make_unique<arrow::Buffer>(finalOut, bitvecLen);
      arrow::ArrayData selectArrayData(arrow::boolean(), tupleSet->numRows(), {nullptr, std::move(selectBuffer)});
      auto expDatum = arrow::compute::Filter(arrow::Datum(tupleSet->table()), arrow::Datum(selectArrayData));
      if (!expDatum.ok()) {
        return tl::make_unexpected(expDatum.status().message());
      }
      filteredTupleSet = TupleSet::make((*expDatum).table());
    }

    // send
    std::shared_ptr<Message> tupleSetMessage = std::make_shared<TupleSetMessage>(filteredTupleSet, name_);
    ctx()->tell(tupleSetMessage);
    numRowsOutput_ += filteredTupleSet->numRows();

    // clear individual hashes and bitvec
    free(distState_.hashesVec32_[i]);
    free(distState_.hashesVec64_[i]);
    for (const auto &out: outVec) {
      free(out);
    }
  }

  // clear processed data
  distState_.tupleSets_.clear();
  distState_.hashesVec32_.clear();
  distState_.hashesVec64_.clear();
  distState_.bitvecLen_.clear();
  distState_.outs_.clear();
  return {};
}

uint8_t* BloomFilterUsePOp::bitvecOr(std::vector<uint8_t*> &bitVecs, int len) {
  // it's guaranteed that the corresponding "bloomFilterValid_" is true, i.e. no nullptr
  // compute "or", store result in the first element
  uint8_t *out = bitVecs[0];
  for (uint i = 1; i < bitVecs.size(); ++i) {
    util::bitmapOr(out, bitVecs[i], len, out);
  }
  return out;
}

void BloomFilterUsePOp::clear() {
  columnIndices_.reset();
  hasher_.reset();
  standAloneState_.clear();
  distState_.clear();
}

#if SHOW_DEBUG_METRICS == true
int64_t BloomFilterUsePOp::getNumRowsInput() const {
  return numRowsInput_;
}
#endif

}
