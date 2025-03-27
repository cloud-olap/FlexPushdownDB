//
// Created by Yifei Yang on 4/16/24.
//

#include <fpdb/executor/physical/bloomfilter/DistGlobalBloomFilterMergePOp.h>
#include <fpdb/executor/physical/bloomfilter/GlobalArrowBloomFilter.h>
#include <fpdb/executor/physical/Globals.h>
#include <thread>

namespace fpdb::executor::physical::bloomfilter {

DistGlobalBloomFilterMergePOp::DistGlobalBloomFilterMergePOp(const std::string &name,
                                                             const std::vector<std::string> &projectColumnNames,
                                                             int nodeId):
  GlobalBloomFilterFinalizePOp(name, projectColumnNames, nodeId) {
  type_ = POpType::DIST_GLOBAL_BLOOM_FILTER_MERGE;
}

std::string DistGlobalBloomFilterMergePOp::getTypeString() const {
  return "DistGlobalBloomFilterMergePOp";
}

void DistGlobalBloomFilterMergePOp::onBloomFilter(const BloomFilterMessage &msg) {
  const auto &bloomFilter = msg.getBloomFilter();
  const auto &remoteInfo = msg.getRemoteInfo();

  // if the bloom filter is from another node
  if (remoteInfo.has_value()) {
    readRemoteBloomFilter(bloomFilter.get(), msg.sender(), *remoteInfo, msg.isRemoteConsumerSpecific());
  }

  if (bloomFilter->getType() != BloomFilterType::GLOBAL_ARROW_BF) {
    ctx()->notifyError("'DistGlobalBloomFilterMergePOp' should only take global Arrow BF.");
    return;
  }
  if (bloomFilter_ == nullptr) {
    bloomFilter_ = bloomFilter;
  } else {
    auto res = merge(bloomFilter);
    if (!res.has_value()) {
      ctx()->notifyError(res.error());
      return;
    }
  }
}

tl::expected<void, std::string> DistGlobalBloomFilterMergePOp::merge(const std::shared_ptr<BloomFilterBase> &other) {
  static constexpr int64_t PARALLEL_THRESH = 1000;
  int64_t numBlocks =
      std::static_pointer_cast<GlobalArrowBloomFilter>(bloomFilter_)->getBlockedBloomFilter()->num_blocks();
  uint numThreads = std::thread::hardware_concurrency();

  if (USE_PARALLEL_DIST_GLOBAL_BF_MERGE && numBlocks >= PARALLEL_THRESH * numThreads ) {
    // FIXME: currently simply use a thread pool for ease of implementation, a standard approach is to leverage actors
    //  for parallelism
    int64_t numBlocksPerPart = numBlocks / numThreads;
    uint remainder = numBlocks % numThreads;
    std::vector<std::thread> thVec{numThreads};
    errVec_.resize(numThreads);
    int64_t blockOffset = 0;
    for (uint i = 0; i < numThreads; ++i) {
      int64_t numBlocksThisPart = numBlocksPerPart + (i < remainder);
      std::thread th(&DistGlobalBloomFilterMergePOp::mergePart, this,
                     std::ref(other), blockOffset, numBlocksThisPart, i);
      thVec[i] = std::move(th);
      blockOffset += numBlocksThisPart;
    }
    for (auto &th: thVec) {
      th.join();
    }
    // check errors
    for (uint i = 0; i < numThreads; ++i) {
      if (!errVec_[i].empty()) {
        return tl::make_unexpected(errVec_[i]);
      }
    }
    errVec_.clear();
  } else {
    auto res = std::static_pointer_cast<GlobalArrowBloomFilter>(bloomFilter_)
            ->merge(std::static_pointer_cast<GlobalArrowBloomFilter>(other));
    if (!res.has_value()) {
      return tl::make_unexpected(res.error());
    }
  }
  return {};
}

void DistGlobalBloomFilterMergePOp::mergePart(
        const std::shared_ptr<BloomFilterBase> &other, int64_t blockOffset, int64_t numBlocks, uint id) {
  auto res = std::static_pointer_cast<GlobalArrowBloomFilter>(bloomFilter_)
          ->mergePart(std::static_pointer_cast<GlobalArrowBloomFilter>(other), blockOffset, numBlocks);
  if (!res.has_value()) {
    errVec_[id] = res.error();
  }
}

}
