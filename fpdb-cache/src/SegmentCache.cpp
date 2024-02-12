//
// Created by matt on 19/5/20.
//

#include <fpdb/cache/SegmentCache.h>
#include <fpdb/cache/policy/LRUCachingPolicy.h>
#include <fpdb/cache/Globals.h>
#include <fmt/format.h>
#include <utility>

using namespace fpdb::cache;

SegmentCache::SegmentCache(std::shared_ptr<CachingPolicy> cachingPolicy) :
	cachingPolicy_(std::move(cachingPolicy)) {
}

std::shared_ptr<SegmentCache> SegmentCache::make(const std::shared_ptr<CachingPolicy>& cachingPolicy) {
  return std::make_shared<SegmentCache>(cachingPolicy);
}

void SegmentCache::store(const std::shared_ptr<SegmentKey> &key, const std::shared_ptr<SegmentData> &data) {
  auto removableKeys = cachingPolicy_->onStore(key);

  if (removableKeys.has_value()) {
    for (auto const &removableKey: *removableKeys.value()) {
      map_.erase(removableKey);
    }
    map_.emplace(key, data);
  }
}

tl::expected<std::shared_ptr<SegmentData>,
			 std::string> SegmentCache::load(const std::shared_ptr<SegmentKey> &key) {

  cachingPolicy_->onLoad(key);

  auto mapIterator = map_.find(key);
  if (mapIterator == map_.end()) {
	  return tl::unexpected(fmt::format("Segment for key '{}' not found", key->toString()));
  } else {
    auto cacheEntry = mapIterator->second;
    return mapIterator->second;
  }
}

unsigned long SegmentCache::remove(const std::shared_ptr<SegmentKey> &key) {
  return map_.erase(key);
}

unsigned long SegmentCache::remove(const std::function<bool(const SegmentKey &)> &predicate) {
  unsigned long erasedCount = 0;
  for (const auto &mapEntry: map_) {
    if (predicate(*mapEntry.first)) {
      cachingPolicy_->onRemove(mapEntry.first);
      map_.erase(mapEntry.first);
      ++erasedCount;
    }
  }
  return erasedCount;
}

size_t SegmentCache::getSize() const {
  return map_.size();
}

std::shared_ptr<std::vector<std::shared_ptr<SegmentKey>>>
SegmentCache::toCache(std::shared_ptr<std::vector<std::shared_ptr<SegmentKey>>> segmentKeys) {
  if (FIX_CACHE_LAYOUT) {
    return std::make_shared<std::vector<std::shared_ptr<SegmentKey>>>();
  }

  return cachingPolicy_->onToCache(std::move(segmentKeys));
}

void SegmentCache::newQuery() {
  cachingPolicy_->onNewQuery();
  clearCrtQueryMetrics();
}

void SegmentCache::clear() {
  map_.clear();
  cachingPolicy_->onClear();
  clearMetrics();
}

size_t SegmentCache::hitNum() const {
  return hitNum_;
}

size_t SegmentCache::missNum() const {
  return missNum_;
}

size_t SegmentCache::shardHitNum() const {
  return shardHitNum_;
}

size_t SegmentCache::shardMissNum() const {
  return shardMissNum_;
}

void SegmentCache::clearMetrics() {
  hitNum_ = 0;
  missNum_ = 0;
  shardHitNum_ = 0;
  shardMissNum_ = 0;
  crtQueryHitNum_ = 0;
  crtQueryMissNum_ = 0;
  crtQueryShardHitNum_ = 0;
  crtQueryShardMissNum_ = 0;
}

const std::shared_ptr<CachingPolicy> &SegmentCache::getCachingPolicy() const {
  return cachingPolicy_;
}

size_t SegmentCache::crtQueryHitNum() const {
  return crtQueryHitNum_;
}

size_t SegmentCache::crtQueryMissNum() const {
  return crtQueryMissNum_;
}

size_t SegmentCache::crtQueryShardHitNum() const {
  return crtQueryShardHitNum_;
}

size_t SegmentCache::crtQueryShardMissNum() const {
  return crtQueryShardMissNum_;
}

void SegmentCache::clearCrtQueryMetrics() {
  crtQueryHitNum_ = 0;
  crtQueryMissNum_ = 0;
  crtQueryShardHitNum_ = 0;
  crtQueryShardMissNum_ = 0;
}

void SegmentCache::addHitNum(size_t hitNum) {
  hitNum_ += hitNum;
}

void SegmentCache::addMissNum(size_t missNum) {
  missNum_ += missNum;
}

void SegmentCache::addShardHitNum(size_t shardHitNum) {
  shardHitNum_ += shardHitNum;
}

void SegmentCache::addShardMissNum(size_t shardMissNum) {
  shardMissNum_ += shardMissNum;
}

void SegmentCache::addCrtQueryHitNum(size_t hitNum) {
  crtQueryHitNum_ += hitNum;
}

void SegmentCache::addCrtQueryMissNum(size_t missNum) {
  crtQueryMissNum_ += missNum;
}

void SegmentCache::addCrtQueryShardHitNum(size_t shardHitNum) {
  crtQueryShardHitNum_ += shardHitNum;
}

void SegmentCache::addCrtQueryShardMissNum(size_t shardMissNum) {
  crtQueryShardMissNum_ += shardMissNum;
}

void SegmentCache::checkCacheConsistencyWithCachePolicy() {
  auto keysInCachePolicy = cachingPolicy_->getKeysetInCachePolicy();
  if (keysInCachePolicy->size() != map_.size()) {
    throw std::runtime_error("Error, cache policy key set has different size than segment cache keyset");
  }

  // make sure all keys in caching policy cache are in segment cache
  // don't need to worry about checking the other way around since we are comparing sets and they are the same size
  // so checking one way will show any errors
  for (const auto& segmentKey : *keysInCachePolicy) {
    if (map_.find(segmentKey) == map_.end()) {
      throw std::runtime_error("Error, cache policy key set has a key not present in the segment cache");
    }
  }
}
