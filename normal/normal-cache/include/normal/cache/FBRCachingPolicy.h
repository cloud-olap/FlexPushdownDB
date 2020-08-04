//
// Created by Yifei Yang on 8/3/20.
//

#ifndef NORMAL_NORMAL_CACHE_INCLUDE_NORMAL_CACHE_FBRCACHINGPOLICY_H
#define NORMAL_NORMAL_CACHE_INCLUDE_NORMAL_CACHE_FBRCACHINGPOLICY_H

#include <memory>
#include <list>
#include <forward_list>
#include <unordered_map>

#include "SegmentKey.h"
#include "CachingPolicy.h"

namespace normal::cache {

class FBRCachingPolicy: public CachingPolicy {

public:
  explicit FBRCachingPolicy(size_t maxSize);
  static std::shared_ptr<FBRCachingPolicy> make(size_t maxSize);

  void onLoad(const std::shared_ptr<SegmentKey> &key) override;
  void onRemove(const std::shared_ptr<SegmentKey> &key) override;
  std::optional<std::shared_ptr<std::vector<std::shared_ptr<SegmentKey> > > > onStore(const std::shared_ptr<SegmentKey> &key) override;
  std::shared_ptr<std::vector<std::shared_ptr<SegmentKey> > > onToCache(std::shared_ptr<std::vector<std::shared_ptr<SegmentKey> > > segmentKeys) override;

};

}


#endif //NORMAL_NORMAL_CACHE_INCLUDE_NORMAL_CACHE_FBRCACHINGPOLICY_H
