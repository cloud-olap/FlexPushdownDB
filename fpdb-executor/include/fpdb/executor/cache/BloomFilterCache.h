//
// Created by Yifei Yang on 9/14/22.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_CACHE_BLOOMFILTERCACHE_HPP
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_CACHE_BLOOMFILTERCACHE_HPP

#include <unordered_map>
#include <string>
#include <shared_mutex>
#include <tl/expected.hpp>

#include "fpdb/executor/physical/bloomfilter/BloomFilterBase.h"

using namespace fpdb::executor::physical::bloomfilter;

namespace fpdb::executor::cache {

/**
 * A cache for constructed bloom filters during bloom filter pushdown, also when broadcasting global bloom filters
 * across the compute nodes.
 */
class BloomFilterCache {

public:
  BloomFilterCache() = default;

  static std::string generateBloomFilterKey(long queryId, const std::string &op);
  static std::string generateBloomFilterKey(long queryId, const std::string &producer, const std::string &consumer);

  tl::expected<std::shared_ptr<BloomFilterBase>, std::string> consumeBloomFilter(const std::string &key);
  void produceBloomFilter(const std::string &key, const std::shared_ptr<BloomFilterBase> &bloomFilter, int num_copies);

private:
  // bloom filter is in the form of <bloom filter, counter>,
  // when the counter reaches 0, the bloom filter will be deleted
  std::unordered_map<std::string, std::pair<std::shared_ptr<BloomFilterBase>, int>> bloom_filters_;
  std::shared_mutex mutex_;

};

}


#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_CACHE_BLOOMFILTERCACHE_HPP
