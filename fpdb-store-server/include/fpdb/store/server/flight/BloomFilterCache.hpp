//
// Created by Yifei Yang on 9/14/22.
//

#ifndef FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_FLIGHT_BLOOMFILTERCACHE_HPP
#define FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_FLIGHT_BLOOMFILTERCACHE_HPP

#include <unordered_map>
#include <string>
#include <shared_mutex>
#include <tl/expected.hpp>

#include "fpdb/executor/physical/bloomfilter/BloomFilterBase.h"

using namespace fpdb::executor::physical::bloomfilter;

namespace fpdb::store::server::flight {

/**
 * A cache for constructed bloom filters during bloom filter pushdown.
 */
class BloomFilterCache {

public:
  BloomFilterCache() = default;

  static std::string generateBloomFilterKey(long queryId, const std::string &op);

  tl::expected<std::shared_ptr<BloomFilterBase>, std::string> consumeBloomFilter(const std::string &key);
  void produceBloomFilter(const std::string &key, const std::shared_ptr<BloomFilterBase> &bloomFilter, int num_copies);

private:
  // bloom filter is in the form of <bloom filter, counter>,
  // when the counter reaches 0, the bloom filter will be deleted
  std::unordered_map<std::string, std::pair<std::shared_ptr<BloomFilterBase>, int>> bloom_filters_;
  std::shared_mutex mutex_;

};

}


#endif //FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_FLIGHT_BLOOMFILTERCACHE_HPP
