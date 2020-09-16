//
// Created by matt on 5/12/19.
//

#include "normal/core/OperatorManager.h"

//#include <graphviz/gvc.h>

#include <normal/core/cache/SegmentCacheActor.h>
#include <normal/core/message/Envelope.h>


using namespace normal::core::cache;
using namespace normal::core::message;

namespace normal::core {

void OperatorManager::start() {
  running_ = true;
}

void OperatorManager::stop() {

  // Send actors a shutdown message
  (*rootActor_)->send_exit(caf::actor_cast<caf::actor>(segmentCacheActor_), caf::exit_reason::user_shutdown);

  // Stop the root actor (seems, being defined by "scope", it needs to actually be destroyed to stop it)
  rootActor_.reset();

  this->actorSystem->await_all_actors_done();

  running_ = false;
}

OperatorManager::OperatorManager() : queryCounter_(0), running_(false){
//  actorSystemConfig.load<caf::io::middleman>();
  actorSystem = std::make_unique<caf::actor_system>(actorSystemConfig);
  rootActor_ = std::make_shared<caf::scoped_actor>(*actorSystem);
}

OperatorManager::OperatorManager(std::shared_ptr<CachingPolicy>  cachingPolicy) :
  cachingPolicy_(std::move(cachingPolicy)),
  queryCounter_(0),
  running_(false){
//  actorSystemConfig.load<caf::io::middleman>();
  actorSystem = std::make_unique<caf::actor_system>(actorSystemConfig);
  rootActor_ = std::make_shared<caf::scoped_actor>(*actorSystem);
}

void OperatorManager::boot() {
  if (cachingPolicy_) {
  	segmentCacheActor_ = actorSystem->spawn(SegmentCacheActor::makeBehaviour, cachingPolicy_);
  } else {
    segmentCacheActor_ = actorSystem->spawn(SegmentCacheActor::makeBehaviour, std::nullopt);
  }
}

std::string OperatorManager::showCacheMetrics() {
  int hitNum;
  int missNum;

  auto errorHandler = [&](const caf::error& error){
	throw std::runtime_error(to_string(error));
  };

  scoped_actor self{*actorSystem};
  self->request(segmentCacheActor_, infinite, GetNumHitsAtom::value).receive(
	  [&](int numHits) {
		hitNum = numHits;
	  },
	  errorHandler);

  self->request(segmentCacheActor_, infinite, GetNumMissesAtom::value).receive(
	  [&](int numMisses) {
		missNum = numMisses;
	  },
	  errorHandler);

  double hitRate = (hitNum + missNum == 0) ? 0.0 : (double) hitNum / (double) (hitNum + missNum);

  std::stringstream ss;
  ss << std::endl;

  ss << std::left << std::setw(60) << "Hit num:";
  ss << std::left << std::setw(40) << hitNum;
  ss << std::endl;

  ss << std::left << std::setw(60) << "Miss num:";
  ss << std::left << std::setw(40) << missNum;
  ss << std::endl;

  ss << std::left << std::setw(60) << "Hit rate:";
  ss << std::left << std::setw(40) << hitRate;
  ss << std::endl;
  ss << std::endl;

  return ss.str();
}

const std::shared_ptr<caf::actor_system> &OperatorManager::getActorSystem() const {
  return actorSystem;
}

const caf::actor &OperatorManager::getSegmentCacheActor() const {
  return segmentCacheActor_;
}

long OperatorManager::nextQueryId() {
  return queryCounter_.fetch_add(1);
}

OperatorManager::~OperatorManager() {
	if(running_)
	  stop();
}

void OperatorManager::clearCacheMetrics() {
  scoped_actor self{*actorSystem};
  self->send(segmentCacheActor_, ClearMetricsAtom::value);
}

}