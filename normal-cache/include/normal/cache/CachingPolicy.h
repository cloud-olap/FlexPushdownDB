//
// Created by matt on 4/6/20.
//

#ifndef NORMAL_NORMAL_CACHE_INCLUDE_NORMAL_CACHE_CACHINGPOLICY_H
#define NORMAL_NORMAL_CACHE_INCLUDE_NORMAL_CACHE_CACHINGPOLICY_H

#include <memory>
#include <optional>
#include <vector>
#include <unordered_set>
#include <normal/plan/mode/Mode.h>

#include "SegmentKey.h"

namespace normal::cache {

enum CachingPolicyId {
  LRU,
  FBR,
  WFBR,
  BELADY
};

class CachingPolicy {

public:

  CachingPolicy(size_t maxSize, std::shared_ptr<normal::plan::operator_::mode::Mode> mode);

  virtual ~CachingPolicy() = default;

  /**
   * Fired on load of an entry
   *
   * @param key
   */
  virtual void onLoad(const std::shared_ptr<SegmentKey> &key) = 0;

   /**
    * Fired on storage of an entry
    *
    * @param key
    * @return A vector of keys to remove from the cache, nullptr if segment cannot be stored
    */
  virtual std::optional<std::shared_ptr<std::vector<std::shared_ptr<SegmentKey>>>> onStore(const std::shared_ptr<SegmentKey> &key) = 0;

  /**
   * Fired on removal of on entry
   *
   * @param key
   */
  virtual void onRemove(const std::shared_ptr<SegmentKey> &key) = 0;

  /**
   * Decide what segments to cache next
   *
   * @param keys of segments to access
   * @return keys of segments to cache next
   */
  virtual std::shared_ptr<std::vector<std::shared_ptr<SegmentKey>>> onToCache(std::shared_ptr<std::vector<std::shared_ptr<SegmentKey>>> segmentKeys) = 0;

  /**
   * Get the keys that the cache policy thinks are stored in the cache
   *
   * @return keys of segments that the cache policy thinks are cached
   */
  virtual std::shared_ptr<std::unordered_set<std::shared_ptr<SegmentKey>, SegmentKeyPointerHash, SegmentKeyPointerPredicate>> getKeysetInCachePolicy() = 0;

  /**
   * Show the current cache layout
   */
  virtual std::string showCurrentLayout() = 0;

  /**
   * Get caching policy id
   *
   * @return caching policy id
   */
  virtual CachingPolicyId id() = 0;

  /**
   * Get caching policy name
   *
   * @return caching policy name
   */
  virtual std::string toString() = 0;

  /**
   * Some updates (FBRS, WFBR) when a new query comes
   */
  virtual void onNewQuery() = 0;

  [[maybe_unused]] [[nodiscard]] size_t getFreeSize() const;

protected:
  std::shared_ptr<normal::plan::operator_::mode::Mode> mode_;
  size_t maxSize_;
  int64_t freeSize_;

public:
  size_t onLoadTime = 0;
  size_t onStoreTime = 0;
  size_t onToCacheTime = 0;
};

// used only in testing math model
inline bool allowFetchSegments = true;

}

#endif //NORMAL_NORMAL_CACHE_INCLUDE_NORMAL_CACHE_CACHINGPOLICY_H
