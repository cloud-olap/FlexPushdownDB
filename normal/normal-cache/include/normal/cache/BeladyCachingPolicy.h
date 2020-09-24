//
// Created by Matt Woicik on 9/22/20.
//

#ifndef NORMAL_NORMAL_CACHE_INCLUDE_NORMAL_CACHE_BELADYCACHINGPOLICY_H
#define NORMAL_NORMAL_CACHE_INCLUDE_NORMAL_CACHE_BELADYCACHINGPOLICY_H

#include <memory>
#include <list>
#include <forward_list>
#include <unordered_set>
#include <unordered_map>
#include <vector>

#include "SegmentKey.h"
#include "CachingPolicy.h"
#include "normal/connector/MiniCatalogue.h"

namespace normal::cache {

// This must be populated with SegmentKey->[Query #s Segment is used in] and
// QueryNumber->[Involved Segment Keys] prior to executing any queries
extern std::shared_ptr<connector::MiniCatalogue> beladyMiniCatalogue;

class BeladyCachingPolicy: public CachingPolicy {

public:
  explicit BeladyCachingPolicy(size_t maxSize, std::shared_ptr<normal::plan::operator_::mode::Mode> mode);
  static std::shared_ptr<BeladyCachingPolicy> make(size_t maxSize, std::shared_ptr<normal::plan::operator_::mode::Mode> mode);

  void onLoad(const std::shared_ptr<SegmentKey> &key) override;
  void onRemove(const std::shared_ptr<SegmentKey> &key) override;
  std::optional<std::shared_ptr<std::vector<std::shared_ptr<SegmentKey>>>> onStore(const std::shared_ptr<SegmentKey> &key) override;
  std::shared_ptr<std::vector<std::shared_ptr<SegmentKey>>> onToCache(std::shared_ptr<std::vector<std::shared_ptr<SegmentKey>>> segmentKeys) override;
  std::string showCurrentLayout() override;

  void generateCacheDecisions(int numQueries);

  CachingPolicyId id() override;

private:
  std::vector<std::shared_ptr<SegmentKey>> keysInCache_;
  // TODO: Change to these to be sets of SegmentKey or some more efficient data structure
  // Not hugely pressing as we are never dealing with that many segments or calling this too many times
  std::unordered_map<int, std::shared_ptr<std::vector<std::shared_ptr<SegmentKey>>>> queryNumToKeysInCache_;

  void erase(const std::shared_ptr<SegmentKey> &key);
};

}

#endif //NORMAL_NORMAL_CACHE_INCLUDE_NORMAL_CACHE_BELADYCACHINGPOLICY_H
