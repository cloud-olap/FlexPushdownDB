//
// Created by matt on 19/5/20.
//

#include "normal/cache/SegmentCache.h"

using namespace normal::cache;

SegmentCache::SegmentCache() = default;

std::shared_ptr<SegmentCache> SegmentCache::make() {
  return std::make_shared<SegmentCache>();
}

void SegmentCache::store(const std::shared_ptr<SegmentKey> &key, const std::shared_ptr<SegmentData> &data) {
  auto cacheEntry = SegmentCacheEntry::make(key, data);
  map_.emplace(key, cacheEntry);
}

tl::expected<std::shared_ptr<SegmentCacheEntry>, std::string> SegmentCache::load(const std::shared_ptr<SegmentKey> &key) {
  auto mapIterator = map_.find(key);
  if(mapIterator == map_.end()){
	return tl::unexpected(fmt::format("Segment for key '{}' not found", key->toString()));
  }
  else{
    auto cacheEntry =  mapIterator->second;
	cacheEntry->setLastUsedTimeStamp(std::chrono::system_clock::now());
	cacheEntry->setUsedCount(cacheEntry->getUsedCount() + 1);
	return mapIterator->second;
  }
}

unsigned long SegmentCache::erase(const std::shared_ptr<SegmentKey> &key) {
  return map_.erase(key);
}

unsigned long SegmentCache::erase(const std::function<bool(const SegmentCacheEntry &)> &predicate) {
  unsigned long erasedCount = 0;
  for(const auto& mapEntry: map_){
	if(predicate(*mapEntry.second)){
	  map_.erase(mapEntry.first);
	  ++erasedCount;
	}
  }
  return erasedCount;
}
