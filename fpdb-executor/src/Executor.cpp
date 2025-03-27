//
// Created by Yifei Yang on 11/23/21.
//

#include <ios>
#include <iomanip>
#include <caf/io/all.hpp>

#include <fpdb/executor/Executor.h>
#include <fpdb/executor/Globals.h>
#include <fpdb/executor/CollAdaptPushdownMetricsExecution.h>
#include <fpdb/cache/caf-serialization/CAFCachingPolicySerializer.h>

using namespace fpdb::executor::cache;
using namespace fpdb::cache;

namespace fpdb::executor {

Executor::Executor(const shared_ptr<::caf::actor_system> &actorSystem,
                   const vector<::caf::node_id> &nodes,
                   const shared_ptr<Mode> &mode,
                   const shared_ptr<CachingPolicy> &cachingPolicy) :
  actorSystem_(actorSystem),
  nodes_(nodes),
  cachingPolicy_(cachingPolicy),
  mode_(mode),
  running_(false) {}

Executor::~Executor() {
  if (running_) {
    stop();
  }
}

void Executor::start() {
  rootActor_ = make_unique<::caf::scoped_actor>(*actorSystem_);
  if ((mode_->id() == CACHING_ONLY || mode_->id() == HYBRID) && !isCacheUsed()) {
    throw runtime_error(fmt::format("Failed to start executor, missing caching policy for mode: {}", mode_->toString()));
  }
  if (isCacheUsed()) {
    localSegmentCacheActor_ = actorSystem_->spawn(SegmentCacheActor::makeBehaviour, cachingPolicy_, mode_);
    for (const auto &node: nodes_) {
      auto remoteSpawnTout = std::chrono::seconds(10);
      auto args = make_message(cachingPolicy_, mode_);
      auto actorTypeName = "SegmentCacheActor";
      auto expectedActorHandle = actorSystem_->middleman().remote_spawn<::caf::actor>(node,
                                                                                      actorTypeName,
                                                                                      args,
                                                                                      remoteSpawnTout);
      if (!expectedActorHandle) {
        throw std::runtime_error(fmt::format("Failed to remote-spawn segment cache actor on node: {}",
                                             to_string(expectedActorHandle.error())));
      }
      remoteSegmentCacheActors_.emplace_back(*expectedActorHandle);
    }
  } else {
    localSegmentCacheActor_ = nullptr;
  }
  running_ = true;
}

void Executor::stop() {
  // Stop the cache actor if cache is used
  // Need to separate "stop" and "exit" messages otherwise remote cache cannot receive (unsure why?)
  if (isCacheUsed()) {
    (*rootActor_)->anon_send(localSegmentCacheActor_, StopCacheAtom_v);
    for (const auto &remoteSegmentCacheActor: remoteSegmentCacheActors_) {
      (*rootActor_)->anon_send(remoteSegmentCacheActor, StopCacheAtom_v);
    }
    // wait a bit if we have remote cache actors
    if (!remoteSegmentCacheActors_.empty()) {
      std::this_thread::sleep_for(100ms);
    }
    (*rootActor_)->send_exit(::caf::actor_cast<::caf::actor>(localSegmentCacheActor_),
                             ::caf::exit_reason::user_shutdown);
    for (const auto &remoteSegmentCacheActor: remoteSegmentCacheActors_) {
      (*rootActor_)->send_exit(::caf::actor_cast<::caf::actor>(remoteSegmentCacheActor),
                               ::caf::exit_reason::user_shutdown);
    }
  }

  // Stop the root actor (seems, being defined by "scope", it needs to actually be destroyed to stop it)
  rootActor_.reset();

  // Wait a bit in distributed exec
  if (!nodes_.empty()) {
    std::this_thread::sleep_for(100ms);
  }

  // Finalize
  this->actorSystem_->await_all_actors_done();
  running_ = false;
}

void Executor::ingestCache(const std::shared_ptr<SegmentCache> &cache) {
  if (localSegmentCacheActor_ == nullptr) {
    throw std::runtime_error("Cannot ingest cache content, \'localSegmentCacheActor_\' is not created");
  }
  (*rootActor_)->anon_send(localSegmentCacheActor_, AssignCacheAtom_v, cache);
}

pair<shared_ptr<TupleSet>, long> Executor::execute(
        long queryId,
        const shared_ptr<PhysicalPlan> &physicalPlan,
        bool isDistributed,
        bool collAdaptPushdownMetrics,
        const std::shared_ptr<fpdb::catalogue::obj_store::FPDBStoreConnector> &fpdbStoreConnector) {
  const auto &execution = collAdaptPushdownMetrics ?
                          make_shared<CollAdaptPushdownMetricsExecution>(queryId,
                                                                         actorSystem_,
                                                                         nodes_,
                                                                         localSegmentCacheActor_,
                                                                         remoteSegmentCacheActors_,
                                                                         physicalPlan,
                                                                         isDistributed,
                                                                         fpdbStoreConnector) :
                          make_shared<Execution>(queryId,
                                                 actorSystem_,
                                                 nodes_,
                                                 localSegmentCacheActor_,
                                                 remoteSegmentCacheActors_,
                                                 physicalPlan,
                                                 isDistributed,
                                                 this);
  execution->execute();

  std::unique_lock lock(ConcurrentOutputMutex);
  // metrics
  if (metrics::hasRegularMetricsToShow()) {
    cout << execution->showRegularMetrics() << endl;
  }
#if SHOW_DEBUG_METRICS == true
  if (metrics::hasDebugMetricsToShow()) {
    cout << execution->showDebugMetrics() << endl;
  }
#endif

  return make_pair(execution->getQueryResult(), execution->getElapsedTime());
}

void Executor::initAdaptExec(long queryId, bool isDistributed) {
  adaptExecs_[queryId] = make_shared<Execution>(queryId,
                                                actorSystem_,
                                                nodes_,
                                                localSegmentCacheActor_,
                                                remoteSegmentCacheActors_,
                                                nullptr,  /*will exec "adaptPhysicalPlan" in each stage*/
                                                isDistributed,
                                                this);
}

void Executor::execNextAdaptStage(long queryId,
                                  const shared_ptr<AdaptPhysicalPlan> &adaptPhysicalPlan) {
  // find the exec
  auto adaptExecIt = adaptExecs_.find(queryId);
  if (adaptExecIt == adaptExecs_.end()) {
    throw runtime_error(fmt::format("Adaptive exec with query id '{}' not found.", queryId));
  }

  // exec this stage
  auto adaptExec = adaptExecIt->second;
  adaptExec->enableAdaptExec(adaptPhysicalPlan);
  adaptExec->execute();
}

pair<shared_ptr<TupleSet>, long> Executor::finishAdaptExec(long queryId) {
  // find the exec
  auto adaptExecIt = adaptExecs_.find(queryId);
  if (adaptExecIt == adaptExecs_.end()) {
    throw runtime_error(fmt::format("Adaptive exec with query id '{}' not found.", queryId));
  }
  auto adaptExec = adaptExecIt->second;

  // clear
  adaptExecs_.erase(adaptExecIt);

  // fetch result
  return make_pair(adaptExec->getQueryResult(), adaptExec->getElapsedTime());
}

const ::caf::actor &Executor::getLocalSegmentCacheActor() const {
  return localSegmentCacheActor_;
}

const vector<::caf::actor> &Executor::getRemoteSegmentCacheActors() const {
  return remoteSegmentCacheActors_;
}

const actor &Executor::getRemoteSegmentCacheActor(int nodeId) const {
  if (nodeId >= (int) nodes_.size()) {
    throw std::runtime_error(fmt::format("Invalid node id '{}' when getting remote segment cache actor, "
                                         "num nodes: '{}'", nodeId, nodes_.size()));
  }
  return remoteSegmentCacheActors_[nodeId];
}

const shared_ptr<::caf::actor_system> &Executor::getActorSystem() const {
  return actorSystem_;
}

bool Executor::isCacheUsed() {
  return cachingPolicy_ != nullptr;
}

std::string Executor::showCacheMetrics() {
  // TODO: add remote cache metrics
  size_t hitNum, missNum;
  size_t shardHitNum, shardMissNum;

  auto errorHandler = [&](const ::caf::error &error) {
    throw std::runtime_error(to_string(error));
  };

  if (!isCacheUsed()) {
    hitNum = 0;
    missNum = 0;
    shardHitNum = 0;
    shardMissNum = 0;
  } else {
    scoped_actor self{*actorSystem_};
    self->request(localSegmentCacheActor_, infinite, GetNumHitsAtom_v).receive(
            [&](size_t numHits) {
              hitNum = numHits;
            },
            errorHandler);

    self->request(localSegmentCacheActor_, infinite, GetNumMissesAtom_v).receive(
            [&](size_t numMisses) {
              missNum = numMisses;
            },
            errorHandler);

    self->request(localSegmentCacheActor_, infinite, GetNumShardHitsAtom_v).receive(
            [&](size_t numShardHits) {
              shardHitNum = numShardHits;
            },
            errorHandler);

    self->request(localSegmentCacheActor_, infinite, GetNumShardMissesAtom_v).receive(
            [&](size_t numShardMisses) {
              shardMissNum = numShardMisses;
            },
            errorHandler);
  }

  double hitRate = (hitNum + missNum == 0) ? 0.0 : (double) hitNum / (double) (hitNum + missNum);
  double shardHitRate = (shardHitNum + shardMissNum == 0) ? 0.0 : (double) shardHitNum / (double) (shardHitNum + shardMissNum);

  std::stringstream ss;
  ss << std::endl;
  ss << std::left << "Local cache metrics:" << std::endl;

  ss << std::left << std::setw(60) << "Hit num:";
  ss << std::left << std::setw(40) << hitNum;
  ss << std::endl;

  ss << std::left << std::setw(60) << "Miss num:";
  ss << std::left << std::setw(40) << missNum;
  ss << std::endl;

  ss << std::left << std::setw(60) << "Hit rate:";
  ss << std::left << std::setw(40) << hitRate;
  ss << std::endl;

  ss << std::left << std::setw(60) << "Shard Hit num:";
  ss << std::left << std::setw(40) << shardHitNum;
  ss << std::endl;

  ss << std::left << std::setw(60) << "Shard Miss num:";
  ss << std::left << std::setw(40) << shardMissNum;
  ss << std::endl;

  ss << std::left << std::setw(60) << "Shard Hit rate:";
  ss << std::left << std::setw(40) << shardHitRate;
  ss << std::endl;

  ss << std::endl;

  return ss.str();
}

void Executor::clearCacheMetrics() {
  if (isCacheUsed()) {
    (*rootActor_)->anon_send(localSegmentCacheActor_, ClearMetricsAtom_v);
    for (const auto &remoteSegmentCacheActor: remoteSegmentCacheActors_) {
      (*rootActor_)->anon_send(remoteSegmentCacheActor, ClearMetricsAtom_v);
    }
  }
}

double Executor::getCrtQueryHitRatio() {
  // TODO: add remote cache metrics
  size_t crtQueryHitNum;
  size_t crtQueryMissNum;

  if (!isCacheUsed()) {
    crtQueryHitNum = 0;
    crtQueryMissNum = 0;
  } else {
    auto errorHandler = [&](const ::caf::error &error) {
      throw std::runtime_error(to_string(error));
    };

    // NOTE: Creating a new scoped_actor will work, but can use rootActor_ as well
    scoped_actor self{*actorSystem_};
    self->request(localSegmentCacheActor_, infinite, GetCrtQueryNumHitsAtom_v).receive(
            [&](size_t numHits) {
              crtQueryHitNum = numHits;
            },
            errorHandler);

    self->request(localSegmentCacheActor_, infinite, GetCrtQueryNumMissesAtom_v).receive(
            [&](size_t numMisses) {
              crtQueryMissNum = numMisses;
            },
            errorHandler);
  }

  return (crtQueryHitNum + crtQueryMissNum == 0) ? 0.0 :
    (double) crtQueryHitNum / (double) (crtQueryHitNum + crtQueryMissNum);
}

double Executor::getCrtQueryShardHitRatio() {
  // TODO: add remote cache metrics
  size_t crtQueryShardHitNum;
  size_t crtQueryShardMissNum;

  if (isCacheUsed()) {
    crtQueryShardHitNum = 0;
    crtQueryShardMissNum = 0;
  } else {
    auto errorHandler = [&](const ::caf::error &error) {
      throw std::runtime_error(to_string(error));
    };

    // NOTE: Creating a new scoped_actor will work, but can use rootActor_ as well
    scoped_actor self{*actorSystem_};
    self->request(localSegmentCacheActor_, infinite, GetCrtQueryNumShardHitsAtom_v).receive(
            [&](size_t numShardHits) {
              crtQueryShardHitNum = numShardHits;
            },
            errorHandler);

    self->request(localSegmentCacheActor_, infinite, GetCrtQueryNumShardMissesAtom_v).receive(
            [&](size_t numShardMisses) {
              crtQueryShardMissNum = numShardMisses;
            },
            errorHandler);
  }

  return (crtQueryShardHitNum + crtQueryShardMissNum == 0) ? 0.0 :
    (double) crtQueryShardHitNum / (double) (crtQueryShardHitNum + crtQueryShardMissNum);
}

}
