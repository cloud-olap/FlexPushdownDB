//
// Created by Yifei Yang on 11/23/22.
//

#include <fpdb/executor/physical/bloomfilter/BloomFilterBase.h>
#include <fpdb/executor/physical/bloomfilter/BloomFilter.h>
#include <fpdb/executor/physical/bloomfilter/ArrowBloomFilter.h>
#include <fmt/format.h>

namespace fpdb::executor::physical::bloomfilter {

BloomFilterBase::BloomFilterBase(BloomFilterType type, int64_t capacity, bool valid):
  type_(type),
  capacity_(capacity),
  valid_(valid) {}

BloomFilterType BloomFilterBase::getType() const {
  return type_;
}

bool BloomFilterBase::valid() const {
  return valid_;
}

tl::expected<std::shared_ptr<BloomFilterBase>, std::string> BloomFilterBase::fromJson(const nlohmann::json &jObj) {
  if (!jObj.contains("type")) {
    return tl::make_unexpected(fmt::format("Type not specified in BloomFilterBase JSON '{}'", to_string(jObj)));
  }
  auto type = jObj["type"].get<BloomFilterType>();

  switch (type) {
    case BLOOM_FILTER: {
      return BloomFilter::fromJson(jObj);
    }
    case ARROW_BLOOM_FILTER: {
      return ArrowBloomFilter::fromJson(jObj);
    }
    default: {
      return tl::make_unexpected(fmt::format("Unknown bloom filter type: {}", type));
    }
  }
}

}
