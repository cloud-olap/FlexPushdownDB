//
// Created by matt on 5/12/19.
//

#include "normal/core/OperatorManager.h"

#include <cassert>
#include <utility>
#include <vector>
//#include <filesystem>
#include <experimental/filesystem>

#include <caf/all.hpp>
#include <caf/io/all.hpp>
//#include <graphviz/gvc.h>

#include <normal/core/Actors.h>
#include "normal/core/Globals.h"
#include "normal/core/message/Envelope.h"
#include "normal/core/Operator.h"
#include "normal/core/message/StartMessage.h"
#include <normal/core/cache/SegmentCacheActor.h>

using namespace normal::core::cache;

namespace normal::core {

//void OperatorManager::put(const std::shared_ptr<normal::core::Operator> &op) {
//
//  assert(op);
//
//  caf::actor rootActorHandle = normal::core::Actors::toActorHandle(this->rootActor_);
//
//  auto ctx = std::make_shared<normal::core::OperatorContext>(op, rootActorHandle);
//  operatorDirectory_.insert(normal::core::OperatorDirectoryEntry(op->name(), ctx, false));
//}

void OperatorManager::start() {

  startTime_ = std::chrono::steady_clock::now();

  // Mark all the operators as incomplete
//  operatorDirectory_.setIncomplete();


//   Send start messages to the actors
//  for (const auto &element: operatorDirectory_) {
//	auto entry = element.second;
//	auto op = entry.getOperatorContext().lock()->op();
//
//    std::vector<caf::actor> actorHandles;
//    for (const auto &consumer: op->consumers())
//      actorHandles.emplace_back(consumer.second.lock()->actorHandle());
//
//    auto sm = std::make_shared<message::StartMessage>(actorHandles, "/root");
//
//    (*rootActor_)->send(op->actorHandle(), normal::core::message::Envelope(sm));
//  }

//  auto sm = std::make_shared<message::StartMessage>(std::vector<caf::actor>{}, "/root");
//  (*rootActor_)->send(actor_cast<actor>(segmentCacheActor_), normal::core::message::Envelope(sm));

  running_ = true;
}

void OperatorManager::stop() {

  // Send actors a shutdown message
//  for (const auto &element: operatorDirectory_) {
//	auto entry = element.second;
//	auto op = entry.getOperatorContext().lock()->op();
//	(*rootActor_)->send_exit(op->actorHandle(), caf::exit_reason::user_shutdown);
//  }
  (*rootActor_)->send_exit(actor_cast<actor>(segmentCacheActor_), caf::exit_reason::user_shutdown);

  // Stop the root actor (seems, being defined by "scope", it needs to actually be destroyed to stop it)
  rootActor_.reset();

  this->actorSystem->await_all_actors_done();

//  this->operatorDirectory_.clear();

  // Destroy the segment cache actor
  // FIXME: Prob better to empty cache rather than kill ptr?
//  segmentCacheActor_.reset();

  stopTime_ = std::chrono::steady_clock::now();

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


/**
 * TODO: Remove this, it no longer applies. Individual queries are now joined.
// */
//void OperatorManager::join() {
//
//  SPDLOG_DEBUG("Waiting for all operators to complete");
//
//  auto handle_err = [&](const caf::error &err) {
//    aout(*rootActor_) << "AUT (actor under test) failed: "
//                      << (*rootActor_)->system().render(err) << std::endl;
//  };
//
//  bool allComplete = false;
//  (*rootActor_)->receive_while([&] { return !allComplete; })(
//      [&](const normal::core::message::Envelope &msg) {
//        SPDLOG_DEBUG("Message received  |  actor: 'OperatorManager', messageKind: '{}', from: '{}'",
//                     msg.message().type(), msg.message().sender());
//
//        this->operatorDirectory_.setComplete(msg.message().sender());
//
//        allComplete = this->operatorDirectory_.allComplete();
//
////        SPDLOG_DEBUG(fmt::format("Operator directory:\n{}", this->operatorDirectory_.showString()));
////        SPDLOG_DEBUG(fmt::format("All operators complete: {}", allComplete));
//      },
//      handle_err);
//}

void OperatorManager::boot() {

  // Create the system actors
//  if (cachingPolicy_) {
//    segmentCacheActor_ = std::make_shared<SegmentCacheActor>("SegmentCache", cachingPolicy_);
//  } else {
//    segmentCacheActor_ = std::make_shared<SegmentCacheActor>("SegmentCache");
//  }
//  put(segmentCacheActor_);

  // Create the operators
//  for (const auto &element: operatorDirectory_) {
//	auto entry = element.second;
//	auto op = entry.getOperatorContext().lock()->op();
//    op->create(entry.getOperatorContext().lock());
//  }

  // Create the operator actors
//  for (const auto &element: operatorDirectory_) {
//	auto entry = element.second;
//	auto op = entry.getOperatorContext().lock()->op();
//    caf::actor actorHandle = actorSystem->spawn<normal::core::OperatorActor>(op);
//    op->actorHandle(actorHandle);
//  }

  if (cachingPolicy_) {
  	segmentCacheActor_ = actorSystem->spawn<SegmentCacheActor>(cachingPolicy_);
  } else {
    segmentCacheActor_ = actorSystem->spawn<SegmentCacheActor>();
  }

  // Tell the actors about the system actors
//  for (const auto &element: operatorDirectory_) {
//	auto entry = element.second;
//	auto op = entry.getOperatorContext().lock()->op();
//
//	auto rootActorEntry = LocalOperatorDirectoryEntry("root",
//											 std::optional(rootActor_->ptr()),
//											 OperatorRelationshipType::None,
//											 false);
//
//	entry.getOperatorContext().lock()->operatorMap().insert(rootActorEntry);
//
//	auto segmentCacheActorEntry = LocalOperatorDirectoryEntry(segmentCacheActor_->name(),
//															  std::optional(segmentCacheActor_->actorHandle()),
//											 OperatorRelationshipType::None,
//											 false);
//
//	entry.getOperatorContext().lock()->operatorMap().insert(segmentCacheActorEntry);
//  }

  // Tell the system actors about the other actors
//  for (const auto &element: operatorDirectory_) {
//	auto entry = element.second;
//	auto op = entry.getOperatorContext().lock()->op();
//
//	auto localEntry = LocalOperatorDirectoryEntry(op->name(),
//											 op->actorHandle(),
//											 OperatorRelationshipType::None,
//											 false);
//
//	segmentCacheActor_->ctx()->operatorMap().insert(localEntry);
//  }

  // Tell the actors who their producers are
//  for (const auto &element: operatorDirectory_) {
//	auto entry = element.second;
//	auto op = entry.getOperatorContext().lock()->op();
//    for (const auto &producerEntry: op->producers()) {
//      auto producer = producerEntry.second;
//      auto localEntry = LocalOperatorDirectoryEntry(producer.lock()->name(),
//                                               producer.lock()->actorHandle(),
//                                               OperatorRelationshipType::Producer,
//                                               false);
//	  entry.getOperatorContext().lock()->operatorMap().insert(localEntry);
//    }
//  }

  // Tell the actors who their consumers are
//  for (const auto &element: operatorDirectory_) {
//	auto entry = element.second;
//	auto op = entry.getOperatorContext().lock()->op();
//    for (const auto &consumerEntry: op->consumers()) {
//      auto consumer = consumerEntry.second;
//      auto localEntry = LocalOperatorDirectoryEntry(consumer.lock()->name(),
//                                               consumer.lock()->actorHandle(),
//                                               OperatorRelationshipType::Consumer,
//                                               false);
//	  entry.getOperatorContext().lock()->operatorMap().insert(localEntry);
//    }
//  }
}

//void OperatorManager::write_graph(const std::string &file) {
//
//  auto gvc = gvContext();
//
//  auto graph = agopen(const_cast<char *>(std::string("Execution Plan").c_str()), Agstrictdirected, 0);
//
//  // Init attributes
//  agattr(graph, AGNODE, const_cast<char *>("fixedsize"), const_cast<char *>("false"));
//  agattr(graph, AGNODE, const_cast<char *>("shape"), const_cast<char *>("ellipse"));
//  agattr(graph, AGNODE, const_cast<char *>("label"), const_cast<char *>("<not set>"));
//  agattr(graph, AGNODE, const_cast<char *>("fontname"), const_cast<char *>("Arial"));
//  agattr(graph, AGNODE, const_cast<char *>("fontsize"), const_cast<char *>("8"));
//
//  // Add all the nodes
//  for (const auto &op: this->m_operatorMap) {
//    std::string nodeName = op.second->op()->name();
//    auto node = agnode(graph, const_cast<char *>(nodeName.c_str()), true);
//
//    agset(node, const_cast<char *>("shape"), const_cast<char *>("plaintext"));
//
//    std::string nodeLabel = "<table border='1' cellborder='0' cellpadding='5'>"
//                            "<tr><td align='left'><b>" + op.second->op()->getType() + "</b></td></tr>"
//                            "<tr><td align='left'>" + op.second->op()->name() + "</td></tr>"
//                            "</table>";
//    char *htmlNodeLabel = agstrdup_html(graph, const_cast<char *>(nodeLabel.c_str()));
//    agset(node, const_cast<char *>("label"), htmlNodeLabel);
//    agstrfree(graph, htmlNodeLabel);
//  }
//
//  // Add all the edges
//  for (const auto &op: this->m_operatorMap) {
//    auto opNode = agfindnode(graph, (char *) (op.second->op()->name().c_str()));
//    for (const auto &c: op.second->op()->consumers()) {
//      auto consumerOpNode = agfindnode(graph, (char *) (c.second->name().c_str()));
//      agedge(graph, opNode, consumerOpNode, const_cast<char *>(std::string("Edge").c_str()), true);
//    }
//  }
//
//  const std::experimental::filesystem::path &path = std::experimental::filesystem::path(file);
//  if (!std::experimental::filesystem::exists(path.parent_path())) {
//    throw std::runtime_error("Could not open file '" + file + "' for writing. Parent directory does not exist");
//  } else {
//    FILE *outFile = fopen(file.c_str(), "w");
//    if (outFile == nullptr) {
//      throw std::runtime_error("Could not open file '" + file + "' for writing. Errno: " + std::to_string(errno));
//    }
//
//    gvLayout(gvc, graph, "dot");
//    gvRender(gvc, graph, "svg", outFile);
//
//    fclose(outFile);
//
//    gvFreeLayout(gvc, graph);
//    agclose(graph);
//    gvFreeContext(gvc);
//  }
//}

tl::expected<long, std::string> OperatorManager::getElapsedTime() {

  if(startTime_.time_since_epoch().count() == 0)
    return tl::unexpected(std::string("Execution time unavailable, query has not been started"));
  if(stopTime_.time_since_epoch().count() == 0)
	return tl::unexpected(std::string("Execution time unavailable, query has not been stopped"));

  return std::chrono::duration_cast<std::chrono::nanoseconds>(stopTime_ - startTime_).count();
}

std::shared_ptr<normal::core::message::Message> OperatorManager::receive() {

  SPDLOG_DEBUG("Waiting for message");

  auto handle_err = [&](const caf::error &err) {
	aout(*rootActor_) << "AUT (actor under test) failed: "
					  << (*rootActor_)->system().render(err) << std::endl;
  };

  std::shared_ptr<normal::core::message::Message> message;

  (*rootActor_)->receive(
	  [&](const normal::core::message::Envelope& msg){
		message = msg.getMessage();
	  },
	  handle_err
  );

  return message;
}

//tl::expected<void, std::string> OperatorManager::send(std::shared_ptr<message::Message> message,
//													  const std::string &recipientId) {
//
//  auto expectedOperator = operatorDirectory_.get(recipientId);
//  if(expectedOperator.has_value()){
//	auto operatorContext = expectedOperator.value().getOperatorContext();
//	auto operatorActor = operatorContext.lock()->operatorActor();
//	(*rootActor_)->send(operatorActor, normal::core::message::Envelope(std::move(message)));
//	return {};
//  }
//  else{
//	return tl::unexpected(fmt::format("Actor with id '{}' not found", recipientId));
//  }
//}

//std::string OperatorManager::showMetrics() {
//
//  std::stringstream ss;
//
//  ss << std::endl;
//
//  long totalProcessingTime = 0;
//  for (auto &entry : operatorDirectory_) {
//	auto processingTime = entry.second.getOperatorContext().lock()->operatorActor()->getProcessingTime();
//	totalProcessingTime += processingTime;
//  }
//
//  auto totalExecutionTime = getElapsedTime().value();
//  std::stringstream formattedExecutionTime;
//  formattedExecutionTime << totalExecutionTime << " \u33B1" << " (" << ((double)totalExecutionTime / 1000000000.0 ) << " secs)";
//  ss << std::left << std::setw(60) << "Total Execution Time ";
//  ss << std::left << std::setw(60) << formattedExecutionTime.str();
//  ss << std::endl;
//  ss << std::endl;
//
//  ss << std::left << std::setw(120) << "Operator Execution Times" << std::endl;
//  ss << std::setfill(' ');
//
//  ss << std::left << std::setw(120) << std::setfill('-') << "" << std::endl;
//  ss << std::setfill(' ');
//
//  ss << std::left << std::setw(60) << "Operator";
//  ss << std::left << std::setw(40) << "Execution Time";
//  ss << std::left << std::setw(20) << "% Total Time";
//  ss << std::endl;
//
//  ss << std::left << std::setw(120) << std::setfill('-') << "" << std::endl;
//  ss << std::setfill(' ');
//
//  for (auto &entry : operatorDirectory_) {
//	auto operatorName = entry.second.name();
//	auto processingTime = entry.second.getOperatorContext().lock()->operatorActor()->getProcessingTime();
//	auto processingFraction = (double)processingTime / (double)totalProcessingTime;
//	std::stringstream formattedProcessingTime;
//	formattedProcessingTime << processingTime << " \u33B1" << " (" << ((double)processingTime / 1000000000.0 ) << " secs)";
//	std::stringstream formattedProcessingPercentage;
//	formattedProcessingPercentage << (processingFraction * 100.0);
//	ss << std::left << std::setw(60) << operatorName;
//	ss << std::left << std::setw(40) << formattedProcessingTime.str();
//	ss << std::left << std::setw(20) << formattedProcessingPercentage.str();
//	ss << std::endl;
//  }
//
//  ss << std::left << std::setw(120) << std::setfill('-') << "" << std::endl;
//  ss << std::setfill(' ');
//
//  std::stringstream formattedProcessingTime;
//  formattedProcessingTime << totalProcessingTime << " \u33B1" << " (" << ((double)totalProcessingTime / 1000000000.0 ) << " secs)";
//  ss << std::left << std::setw(60) << "Total ";
//  ss << std::left << std::setw(40) << formattedProcessingTime.str();
//  ss << std::left << std::setw(20) << "100.0";
//  ss << std::endl;
//  ss << std::endl;
//
//  return ss.str();
//}

std::string OperatorManager::showCacheMetrics() {

//  int hitNum = segmentCacheActor_->getState()->cache->hitNum();
//  int missNum = segmentCacheActor_->getState()->cache->missNum();

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
//  segmentCacheActor_->getState()->cache->clearMetrics();
  scoped_actor self{*actorSystem};
  self->send(segmentCacheActor_, ClearMetricsAtom::value);
}

}