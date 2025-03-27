//
// Created by Yifei Yang on 10/24/23.
//

#include <fpdb/executor/physical/bloomfilter/GlobalBloomFilterCreateArrowKernel.h>

namespace fpdb::executor::physical::bloomfilter {

GlobalBloomFilterCreateArrowKernel::GlobalBloomFilterCreateArrowKernel(const std::vector<std::string> &columnNames):
  BloomFilterCreateAbstractKernel(BloomFilterCreateKernelType::GLOBAL_ARROW_KERNEL, columnNames){}

std::shared_ptr<GlobalBloomFilterCreateArrowKernel>
GlobalBloomFilterCreateArrowKernel::make(const std::vector<std::string> &columnNames) {
  return std::make_shared<GlobalBloomFilterCreateArrowKernel>(columnNames);
}

void GlobalBloomFilterCreateArrowKernel::setInitedGlobalArrowBloomFilter(
        int threadId,
        const std::shared_ptr<GlobalArrowBloomFilter> &bloomFilter) {
  threadId_ = threadId;
  bloomFilter_ = bloomFilter;
}

tl::expected<void, std::string> GlobalBloomFilterCreateArrowKernel::buildBloomFilter() {
  // check
  if (!receivedTupleSet_.has_value()) {
    return tl::make_unexpected("No tupleSet received");
  }
  if (!(*bloomFilter_)->valid()) {
    return {};
  }

  // make hasher
  auto expHasher = RecordBatchHasher::make((*receivedTupleSet_)->schema(), columnNames_);
  if (!expHasher.has_value()) {
    return tl::make_unexpected(expHasher.error());
  }

  // build this part of the global Arrow bf
  auto res = (*bloomFilter_)->build(*expHasher, *receivedTupleSet_, threadId_);
  if (!res.has_value()) {
    return tl::make_unexpected(res.error());
  }

  return {};
}

std::optional<std::shared_ptr<BloomFilterBase>> GlobalBloomFilterCreateArrowKernel::getBloomFilter() const {
  return bloomFilter_;
}

void GlobalBloomFilterCreateArrowKernel::clear() {
  BloomFilterCreateAbstractKernel::clear();
  bloomFilter_.reset();
}

}
