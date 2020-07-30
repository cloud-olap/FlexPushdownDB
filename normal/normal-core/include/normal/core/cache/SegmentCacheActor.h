//
// Created by matt on 21/5/20.
//

#ifndef NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_CACHE_SEGMENTCACHEACTOR_H
#define NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_CACHE_SEGMENTCACHEACTOR_H

#include <normal/core/Operator.h>

#include <caf/all.hpp>

#include <normal/cache/SegmentCache.h>

#include <normal/core/Globals.h>

#include <normal/core/cache/LoadRequestMessage.h>
#include <normal/core/cache/StoreRequestMessage.h>
#include <normal/core/cache/EvictRequestMessage.h>
#include "SegmentCacheActorState.h"
#include "LoadResponseMessage.h"

using namespace caf;
using namespace normal::cache;

namespace normal::core::cache {

class SegmentCacheActor : public normal::core::Operator {

public:

  SegmentCacheActor(const std::string &Name);
  SegmentCacheActor(const std::string &Name, const std::shared_ptr<CachingPolicy>& cachingPolicy);

  void onReceive(const message::Envelope &message) override;

  void load(const LoadRequestMessage &msg);
  void store(const StoreRequestMessage &msg);
  void evict(const EvictRequestMessage &msg);

private:
  std::shared_ptr<SegmentCacheActorState> state_;

};

}

#endif //NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_CACHE_SEGMENTCACHEACTOR_H
