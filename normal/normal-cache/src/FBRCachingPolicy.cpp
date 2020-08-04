//
// Created by Yifei Yang on 8/3/20.
//

#include "normal/cache/FBRCachingPolicy.h"

using namespace normal::cache;

FBRCachingPolicy::FBRCachingPolicy(size_t maxSize) : CachingPolicy(maxSize) {}

void FBRCachingPolicy::onLoad(const std::shared_ptr<SegmentKey> &key) {

}

void FBRCachingPolicy::onRemove(const std::shared_ptr<SegmentKey> &key) {

}

std::optional<std::shared_ptr<std::vector<std::shared_ptr<SegmentKey> > > >
FBRCachingPolicy::onStore(const std::shared_ptr<SegmentKey> &key) {
  return std::optional<std::shared_ptr<std::vector<std::shared_ptr<SegmentKey>>>>();
}

std::shared_ptr<std::vector<std::shared_ptr<SegmentKey> > >
FBRCachingPolicy::onToCache(std::shared_ptr<std::vector<std::shared_ptr<SegmentKey>>> segmentKeys) {
  return std::shared_ptr<std::vector<std::shared_ptr<SegmentKey>>>();
}

std::shared_ptr<FBRCachingPolicy> FBRCachingPolicy::make(size_t maxSize) {
  return std::shared_ptr<FBRCachingPolicy>();
}
