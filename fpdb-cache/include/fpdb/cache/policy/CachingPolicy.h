//
// Created by matt on 4/6/20.
//

#ifndef FPDB_FPDB_CACHE_INCLUDE_FPDB_CACHE_CACHINGPOLICY_H
#define FPDB_FPDB_CACHE_INCLUDE_FPDB_CACHE_CACHINGPOLICY_H

#include <fpdb/cache/SegmentKey.h>
#include <fpdb/catalogue/CatalogueEntry.h>
#include <memory>
#include <optional>
#include <vector>
#include <unordered_set>

using namespace fpdb::catalogue;
using namespace fpdb::cache;

namespace fpdb::cache::policy {

enum CachingPolicyType {
  LRU,
  LFU,
  LFUS,
  WLFU,
  BELADY,
  NONE
};

class CachingPolicy {

public:

  CachingPolicy(CachingPolicyType type,
                size_t maxSize,
                std::shared_ptr<CatalogueEntry> catalogueEntry,
                bool readSegmentSize);
  CachingPolicy() = default;
  CachingPolicy(const CachingPolicy&) = default;
  CachingPolicy& operator=(const CachingPolicy&) = default;

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
   * Get caching policy name
   *
   * @return caching policy name
   */
  virtual std::string toString() = 0;

  /**
   * Some updates (LFUS, WLFU) when a new query comes
   */
  virtual void onNewQuery() = 0;

  /**
   * Clear
   */
  virtual void onClear();

  size_t getFreeSize() const;
  CachingPolicyType getType() const;

  size_t getSegmentSize(const std::shared_ptr<SegmentKey>& segmentKey) const;
  const unordered_map<std::shared_ptr<SegmentKey>, size_t, SegmentKeyPointerHash, 
    SegmentKeyPointerPredicate> &getSegmentSizeMap() const;

protected:
  CachingPolicyType type_;
  size_t maxSize_;
  size_t freeSize_;

  std::shared_ptr<CatalogueEntry> catalogueEntry_;
  std::unordered_map<std::shared_ptr<SegmentKey>, size_t,
          SegmentKeyPointerHash, SegmentKeyPointerPredicate> segmentSizeMap_;

public:
  size_t onLoadTime = 0;
  size_t onStoreTime = 0;
  size_t onToCacheTime = 0;
};

// used only in testing math model
inline bool allowFetchSegments = true;

}

#endif //FPDB_FPDB_CACHE_INCLUDE_FPDB_CACHE_CACHINGPOLICY_H
