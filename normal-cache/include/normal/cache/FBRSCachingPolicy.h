//
// Created by ec2-user on 12/24/20.
//

#ifndef NORMAL_NORMAL_CACHE_INCLUDE_NORMAL_CACHE_FBRSCACHINGPOLICY_H
#define NORMAL_NORMAL_CACHE_INCLUDE_NORMAL_CACHE_FBRSCACHINGPOLICY_H

#include <memory>
#include <unordered_set>
#include <unordered_map>
#include <vector>
#include <queue>

#include "SegmentKey.h"
#include "CachingPolicy.h"

namespace normal::cache {

class FBRSCachingPolicy: public CachingPolicy {

public:
  explicit FBRSCachingPolicy(size_t maxSize, std::shared_ptr<normal::plan::operator_::mode::Mode> mode);
  static std::shared_ptr<FBRSCachingPolicy> make(size_t maxSize, std::shared_ptr<normal::plan::operator_::mode::Mode> mode);

  void onLoad(const std::shared_ptr<SegmentKey> &key) override;
  void onRemove(const std::shared_ptr<SegmentKey> &key) override;
  std::optional<std::shared_ptr<std::vector<std::shared_ptr<SegmentKey>>>> onStore(const std::shared_ptr<SegmentKey> &key) override;
  std::shared_ptr<std::vector<std::shared_ptr<SegmentKey>>> onToCache(std::shared_ptr<std::vector<std::shared_ptr<SegmentKey>>> segmentKeys) override;
  std::string showCurrentLayout() override;
  std::shared_ptr<std::unordered_set<std::shared_ptr<SegmentKey>, SegmentKeyPointerHash, SegmentKeyPointerPredicate>> getKeysetInCachePolicy() override;
  CachingPolicyId id() override;
  std::string toString() override;
  void onNewQuery() override;

private:
  std::vector<std::shared_ptr<SegmentKey>> keysInCache_;
  std::unordered_set<std::shared_ptr<SegmentKey>, SegmentKeyPointerHash, SegmentKeyPointerPredicate> keySetInCache_;
  std::unordered_set<std::shared_ptr<SegmentKey>, SegmentKeyPointerHash, SegmentKeyPointerPredicate> keySet_;

  /**
   * tmp data structures for onToCache()  (OTC: onToCache)
   */
  size_t freeSizeOTC_{};
  std::vector<std::shared_ptr<SegmentKey>> keysInCacheOTC_;

  static bool lessValue(const std::shared_ptr<SegmentKey> &key1, const std::shared_ptr<SegmentKey> &key2);

  /**
   * For FBR, erasing only erases the element in keyInCache_, but not in keySet_ to keep history hitNum
   */
  void erase(const std::shared_ptr<SegmentKey> &key);
};

}

#endif //NORMAL_NORMAL_CACHE_INCLUDE_NORMAL_CACHE_FBRSCACHINGPOLICY_H
