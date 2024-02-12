//
// Created by Yifei Yang on 11/24/22.
//

#include <fpdb/executor/physical/bloomfilter/BloomFilterCreateArrowKernel.h>

namespace fpdb::executor::physical::bloomfilter {

BloomFilterCreateArrowKernel::BloomFilterCreateArrowKernel(const std::vector<std::string> &columnNames):
  BloomFilterCreateAbstractKernel(BloomFilterCreateKernelType::BLOOM_FILTER_ARROW_KERNEL, columnNames){}

std::shared_ptr<BloomFilterCreateArrowKernel>
BloomFilterCreateArrowKernel::make(const std::vector<std::string> &columnNames) {
  return std::make_shared<BloomFilterCreateArrowKernel>(columnNames);
}

tl::expected<void, std::string> BloomFilterCreateArrowKernel::buildBloomFilter() {
  // check
  if (!receivedTupleSet_.has_value()) {
    return tl::make_unexpected("No tupleSet received");
  }
  bloomFilter_ = std::make_shared<ArrowBloomFilter>((*receivedTupleSet_)->numRows(), columnNames_);
  if (!(*bloomFilter_)->valid()) {
    return {};
  }

  // build bloom filter
  auto res = (*bloomFilter_)->build(*receivedTupleSet_);
  if (!res.has_value()) {
    return res;
  }

  return {};
}

std::optional<std::shared_ptr<BloomFilterBase>> BloomFilterCreateArrowKernel::getBloomFilter() const {
  return bloomFilter_;
}

void BloomFilterCreateArrowKernel::clear() {
  BloomFilterCreateAbstractKernel::clear();
  bloomFilter_.reset();
}

}
