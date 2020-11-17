//
// Created by matt on 19/5/20.
//

#ifndef NORMAL_NORMAL_CACHE_INCLUDE_NORMAL_CACHE_SEGMENTCACHE_H
#define NORMAL_NORMAL_CACHE_INCLUDE_NORMAL_CACHE_SEGMENTCACHE_H

#include <unordered_map>
#include <memory>

#include "SegmentKey.h"
#include "SegmentData.h"
#include "CachingPolicy.h"

namespace normal::cache {

class SegmentCache {

public:
  explicit SegmentCache(std::shared_ptr<CachingPolicy> cachingPolicy_);

  static std::shared_ptr<SegmentCache> make();
  static std::shared_ptr<SegmentCache> make(const std::shared_ptr<CachingPolicy>& cachingPolicy);

  void store(const std::shared_ptr<SegmentKey>& key, const std::shared_ptr<SegmentData>& data);
  tl::expected<std::shared_ptr<SegmentData>, std::string> load(const std::shared_ptr<SegmentKey>& key);
  unsigned long remove(const std::shared_ptr<SegmentKey>& key);
  unsigned long remove(const std::function<bool(const SegmentKey& entry)>& predicate);
  std::shared_ptr<std::vector<std::shared_ptr<SegmentKey>>> toCache(std::shared_ptr<std::vector<std::shared_ptr<SegmentKey>>> segmentKeys);

  size_t getSize() const;
  const std::shared_ptr<CachingPolicy> &getCachingPolicy() const;
  int hitNum() const;
  int missNum() const;
  void addHitNum(size_t hitNum);
  void addMissNum(size_t missNum);
  int crtQueryHitNum() const;
  int crtQueryMissNum() const;
  void addCrtQueryHitNum(size_t hitNum);
  void addCrtQueryMissNum(size_t missNum);
  void clearMetrics();
  void clearCrtQueryMetrics();

private:
  void checkCacheConsistensyWithCachePolicy();

  std::unordered_map<std::shared_ptr<SegmentKey>, std::shared_ptr<SegmentData>, SegmentKeyPointerHash, SegmentKeyPointerPredicate> map_;
	std::shared_ptr<CachingPolicy> cachingPolicy_;
	int hitNum_ = 0;
	int missNum_ = 0;
	int crtQueryHitNum_ = 0;
	int crtQueryMissNum_ = 0;
};

}

#endif //NORMAL_NORMAL_CACHE_INCLUDE_NORMAL_CACHE_SEGMENTCACHE_H
