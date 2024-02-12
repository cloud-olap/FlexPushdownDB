//
// Created by matt on 21/5/20.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_CACHE_SEGMENTCACHEACTOR_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_CACHE_SEGMENTCACHEACTOR_H

#include <fpdb/executor/message/cache/LoadRequestMessage.h>
#include <fpdb/executor/message/cache/LoadResponseMessage.h>
#include <fpdb/executor/message/cache/StoreRequestMessage.h>
#include <fpdb/executor/message/cache/WeightRequestMessage.h>
#include <fpdb/executor/message/cache/CacheMetricsMessage.h>
#include <fpdb/executor/caf-serialization/CAFMessageSerializer.h>
#include <fpdb/cache/policy/CachingPolicy.h>
#include <fpdb/cache/SegmentCache.h>
#include <fpdb/plan/Mode.h>
#include <fpdb/caf/CAFUtil.h>

using namespace fpdb::executor::message;
using namespace fpdb::cache;
using namespace fpdb::plan;
using namespace caf;

CAF_BEGIN_TYPE_ID_BLOCK(SegmentCacheActor, fpdb::caf::CAFUtil::SegmentCacheActor_first_custom_type_id)
CAF_ADD_ATOM(SegmentCacheActor, LoadAtom)
CAF_ADD_ATOM(SegmentCacheActor, StoreAtom)
CAF_ADD_ATOM(SegmentCacheActor, WeightAtom)
CAF_ADD_ATOM(SegmentCacheActor, NewQueryAtom)
CAF_ADD_ATOM(SegmentCacheActor, StopCacheAtom)
CAF_ADD_ATOM(SegmentCacheActor, GetNumHitsAtom)
CAF_ADD_ATOM(SegmentCacheActor, GetNumMissesAtom)
CAF_ADD_ATOM(SegmentCacheActor, GetNumShardHitsAtom)
CAF_ADD_ATOM(SegmentCacheActor, GetNumShardMissesAtom)
CAF_ADD_ATOM(SegmentCacheActor, GetCrtQueryNumHitsAtom)
CAF_ADD_ATOM(SegmentCacheActor, GetCrtQueryNumMissesAtom)
CAF_ADD_ATOM(SegmentCacheActor, GetCrtQueryNumShardHitsAtom)
CAF_ADD_ATOM(SegmentCacheActor, GetCrtQueryNumShardMissesAtom)
CAF_ADD_ATOM(SegmentCacheActor, ClearMetricsAtom)
CAF_ADD_ATOM(SegmentCacheActor, MetricsAtom)
CAF_END_TYPE_ID_BLOCK(SegmentCacheActor)

namespace fpdb::executor::cache {

struct SegmentCacheActorState {
  std::string name = "segment-cache";
  std::shared_ptr<SegmentCache> cache;
};

class SegmentCacheActor {

public:
  static behavior makeBehaviour(stateful_actor<SegmentCacheActorState> *self,
                                std::shared_ptr<CachingPolicy> cachingPolicy,
                                std::shared_ptr<Mode> mode);

  static std::shared_ptr<LoadResponseMessage> load(const LoadRequestMessage &msg,
                                                   stateful_actor<SegmentCacheActorState> *self,
                                                   const std::shared_ptr<Mode> &mode);
  static void store(const StoreRequestMessage &msg, stateful_actor<SegmentCacheActorState> *self);
  static void weight(const WeightRequestMessage &msg, stateful_actor<SegmentCacheActorState> *self);
  static void metrics(const CacheMetricsMessage &msg, stateful_actor<SegmentCacheActorState> *self);

private:
  static void stop(stateful_actor<SegmentCacheActorState> *self);

};

}

#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_CACHE_SEGMENTCACHEACTOR_H
