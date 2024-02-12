//
// Created by matt on 19/5/20.
//

#ifndef FPDB_FPDB_CACHE_INCLUDE_FPDB_CACHE_SEGMENTCACHE_H
#define FPDB_FPDB_CACHE_INCLUDE_FPDB_CACHE_SEGMENTCACHE_H

#include <fpdb/cache/SegmentKey.h>
#include <fpdb/cache/SegmentData.h>
#include <fpdb/cache/policy/CachingPolicy.h>
#include <unordered_map>
#include <memory>

using namespace fpdb::cache::policy;

namespace fpdb::cache {

class SegmentCache {

public:
  explicit SegmentCache(std::shared_ptr<CachingPolicy> cachingPolicy_);

  static std::shared_ptr<SegmentCache> make(const std::shared_ptr<CachingPolicy>& cachingPolicy);

  void store(const std::shared_ptr<SegmentKey>& key, const std::shared_ptr<SegmentData>& data);
  tl::expected<std::shared_ptr<SegmentData>, std::string> load(const std::shared_ptr<SegmentKey>& key);
  unsigned long remove(const std::shared_ptr<SegmentKey>& key);
  unsigned long remove(const std::function<bool(const SegmentKey& entry)>& predicate);
  std::shared_ptr<std::vector<std::shared_ptr<SegmentKey>>> toCache(std::shared_ptr<std::vector<std::shared_ptr<SegmentKey>>> segmentKeys);
  void newQuery();
  void clear();

  size_t getSize() const;
  const std::shared_ptr<CachingPolicy> &getCachingPolicy() const;
  size_t hitNum() const;
  size_t missNum() const;
  size_t shardHitNum() const;
  size_t shardMissNum() const;
  void addHitNum(size_t hitNum);
  void addMissNum(size_t missNum);
  void addShardHitNum(size_t shardHitNum);
  void addShardMissNum(size_t shardMissNum);
  size_t crtQueryHitNum() const;
  size_t crtQueryMissNum() const;
  size_t crtQueryShardHitNum() const;
  size_t crtQueryShardMissNum() const;
  void addCrtQueryHitNum(size_t hitNum);
  void addCrtQueryMissNum(size_t missNum);
  void addCrtQueryShardHitNum(size_t shardHitNum);
  void addCrtQueryShardMissNum(size_t shardMissNum);
  void clearMetrics();
  void clearCrtQueryMetrics();

private:
  void checkCacheConsistencyWithCachePolicy();

  std::unordered_map<std::shared_ptr<SegmentKey>, std::shared_ptr<SegmentData>, SegmentKeyPointerHash, SegmentKeyPointerPredicate> map_;
	std::shared_ptr<CachingPolicy> cachingPolicy_;
  size_t hitNum_ = 0;
  size_t missNum_ = 0;
  size_t shardHitNum_ = 0;
  size_t shardMissNum_ = 0;
  size_t crtQueryHitNum_ = 0;
  size_t crtQueryMissNum_ = 0;
  size_t crtQueryShardHitNum_ = 0;
  size_t crtQueryShardMissNum_ = 0;
};

}

#endif //FPDB_FPDB_CACHE_INCLUDE_FPDB_CACHE_SEGMENTCACHE_H
