//
// Created by Yifei Yang on 9/10/20.
//

#ifndef NORMAL_NORMAL_CACHE_INCLUDE_NORMAL_CACHE_WFBRCACHINGPOLICY_H
#define NORMAL_NORMAL_CACHE_INCLUDE_NORMAL_CACHE_WFBRCACHINGPOLICY_H

#include <vector>
#include <unordered_set>
#include <unordered_map>
#include "CachingPolicy.h"

namespace normal::cache {

class WFBRCachingPolicy : public CachingPolicy {

public:
  explicit WFBRCachingPolicy(size_t maxSize, std::shared_ptr<normal::plan::operator_::mode::Mode> mode);
  static std::shared_ptr<WFBRCachingPolicy> make(size_t maxSize, std::shared_ptr<normal::plan::operator_::mode::Mode> mode);

  void onLoad(const std::shared_ptr<SegmentKey> &key) override;
  void onWeight(const std::shared_ptr<std::unordered_map<std::shared_ptr<SegmentKey>, double>> &weightMap, long queryId);
  void onRemove(const std::shared_ptr<SegmentKey> &key) override;
  std::optional<std::shared_ptr<std::vector<std::shared_ptr<SegmentKey>>>> onStore(const std::shared_ptr<SegmentKey> &key) override;
  std::shared_ptr<std::vector<std::shared_ptr<SegmentKey>>> onToCache(std::shared_ptr<std::vector<std::shared_ptr<SegmentKey>>> segmentKeys) override;
  std::shared_ptr<std::unordered_set<std::shared_ptr<SegmentKey>, SegmentKeyPointerHash, SegmentKeyPointerPredicate>> getKeysetInCachePolicy() override;
  std::string showCurrentLayout() override;
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

  /**
   * WeightRequestMessage are sent from both S3SelectScan and filter in hybrid caching, they are contain weight
   * on predicate columns, but we should only update once
   */
  long currentQueryId_;
  std::unordered_set<std::shared_ptr<SegmentKey>, SegmentKeyPointerHash, SegmentKeyPointerPredicate> weightUpdatedKeys_;

  static bool lessValue(const std::shared_ptr<SegmentKey> &key1, const std::shared_ptr<SegmentKey> &key2);

  /**
   * For WFBR, erasing only erases the element in keyInCache_, but not in keySet_ to keep history hitNum
   */
  void erase(const std::shared_ptr<SegmentKey> &key);
};

}

#endif //NORMAL_NORMAL_CACHE_INCLUDE_NORMAL_CACHE_WFBRCACHINGPOLICY_H
