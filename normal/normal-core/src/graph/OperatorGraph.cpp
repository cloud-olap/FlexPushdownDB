//
// Created by matt on 7/7/20.
//

#include "normal/core/graph/OperatorGraph.h"

#include <cassert>

#include <caf/all.hpp>
#include <experimental/filesystem>
//#include <graphviz/gvc.h>

#include <normal/core/Actors.h>
#include <normal/core/OperatorDirectoryEntry.h>
#include <normal/core/Globals.h>

using namespace normal::core::graph;
using namespace normal::core;
using namespace std::experimental;

graph::OperatorGraph::OperatorGraph(long id, const std::shared_ptr<OperatorManager>& operatorManager) : id_(id), operatorManager_(operatorManager) {
  rootActor_ = std::make_shared<caf::scoped_actor>(*operatorManager->getActorSystem());
}

std::shared_ptr<OperatorGraph> graph::OperatorGraph::make(const std::shared_ptr<OperatorManager>& operatorManager) {
  return std::make_shared<OperatorGraph>(operatorManager->nextQueryId(), operatorManager);
}

void graph::OperatorGraph::put(const std::shared_ptr<Operator> &op) {

  assert(op);

  caf::actor rootActorHandle = Actors::toActorHandle(this->rootActor_);

  auto ctx = std::make_shared<normal::core::OperatorContext>(op, rootActorHandle);
  m_operatorMap.insert(std::pair(op->name(), ctx));

  operatorDirectory_.insert(OperatorDirectoryEntry(op->name(), std::nullopt, false));
}

void graph::OperatorGraph::startOperatorAndProducers(const std::shared_ptr<Operator>& op, std::unordered_map<std::string, bool> operatorStates){

  if(!operatorStates[op->name()]) {
	std::vector<caf::actor> actorHandles;
	for (const auto &consumer: op->consumers())
	  actorHandles.emplace_back(consumer.second->actorHandle());

	auto sm = std::make_shared<normal::core::message::StartMessage>(actorHandles,
																	fmt::format("/query-{}/{}",
																				this->id_,
																				GraphRootActorName));
	(*rootActor_)->send(op->actorHandle(), normal::core::message::Envelope(sm));

	operatorStates.emplace(op->name(), true);

	for (const auto &producer: op->producers()) {
	  startOperatorAndProducers(producer.second, operatorStates);
	}
  }
}

void graph::OperatorGraph::start() {

  startTime_ = std::chrono::steady_clock::now();

  // Mark all the operators as incomplete
  operatorDirectory_.setIncomplete();

  // Traverse the graph from consumer to producer, keeping track of which have been sent start messages
  std::unordered_map<std::string, bool> operatorStates;
  for(const auto &op: m_operatorMap){
	operatorStates.emplace(op.first, false);
  }

  int index = 1;
  for(const auto &entry: m_operatorMap){
    auto ctx = entry.second;
    auto op = ctx->op();

//    SPDLOG_INFO("Start: {}", index++);
//    if(op->consumers().empty()){
//      startOperatorAndProducers(op, operatorStates);
//    }

    // just start one by one
    std::vector<caf::actor> actorHandles;
    for (const auto &consumer: op->consumers())
      actorHandles.emplace_back(consumer.second->actorHandle());
    auto sm = std::make_shared<normal::core::message::StartMessage>(actorHandles,
                                                                    fmt::format("/query-{}/{}",
                                                                                this->id_,
                                                                                GraphRootActorName));
    (*rootActor_)->send(op->actorHandle(), normal::core::message::Envelope(sm));
    operatorStates.emplace(op->name(), true);

  }
}

void graph::OperatorGraph::join() {

  SPDLOG_DEBUG("Waiting for all operators to complete");

  auto handle_err = [&](const caf::error &err) {
	aout(*rootActor_) << "AUT (actor under test) failed: "
					  << (*rootActor_)->system().render(err) << std::endl;
  };

  bool allComplete = false;
  (*rootActor_)->receive_while([&] { return !allComplete; })(
	  [&](const normal::core::message::Envelope &msg) {
		SPDLOG_DEBUG("Query root actor received message  |  query: '{}', messageKind: '{}', from: '{}'",
					 this->getId(), msg.message().type(), msg.message().sender());

		this->operatorDirectory_.setComplete(msg.message().sender());

		allComplete = this->operatorDirectory_.allComplete();

//        SPDLOG_DEBUG(fmt::format("Operator directory:\n{}", this->operatorDirectory_.showString()));
//        SPDLOG_DEBUG(fmt::format("All operators complete: {}", allComplete));
	  },
	  handle_err);

  stopTime_ = std::chrono::steady_clock::now();
}

void graph::OperatorGraph::boot() {

  // Create the operators
  for (const auto &element: m_operatorMap) {
	auto ctx = element.second;
	auto op = ctx->op();
	op->create(ctx);
  }

  // Create the operator actors
  for (const auto &element: m_operatorMap) {
	auto ctx = element.second;
	auto op = ctx->op();
	caf::actor actorHandle = operatorManager_->getActorSystem()->spawn<normal::core::OperatorActor>(op);
	op->actorHandle(actorHandle);
  }

  // Tell the actors about the system actors
  for (const auto &element: m_operatorMap) {

	auto ctx = element.second;
	auto op = ctx->op();

	auto rootActorEntry = LocalOperatorDirectoryEntry(GraphRootActorName,
													  std::optional(rootActor_->ptr()),
													  OperatorRelationshipType::None,
													  false);

	ctx->operatorMap().insert(rootActorEntry);

	auto segmentCacheActorEntry = LocalOperatorDirectoryEntry(operatorManager_->getSegmentCacheActor()->name(),
															  std::optional(operatorManager_->getSegmentCacheActor()->actorHandle()),
															  OperatorRelationshipType::None,
															  false);

	ctx->operatorMap().insert(segmentCacheActorEntry);
  }

  // Tell the system actors about the other actors
  for (const auto &element: m_operatorMap) {

	auto ctx = element.second;
	auto op = ctx->op();

	auto entry = LocalOperatorDirectoryEntry(op->name(),
											 op->actorHandle(),
											 OperatorRelationshipType::None,
											 false);

	operatorManager_->getSegmentCacheActor()->ctx()->operatorMap().insert(entry);
  }

  // Tell the actors who their producers are
  for (const auto &element: m_operatorMap) {
	auto ctx = element.second;
	auto op = ctx->op();
	for (const auto &producerEntry: op->producers()) {
	  auto producer = producerEntry.second;
	  auto entry = LocalOperatorDirectoryEntry(producer->name(),
											   producer->actorHandle(),
											   OperatorRelationshipType::Producer,
											   false);
	  ctx->operatorMap().insert(entry);
	}
  }

  // Tell the actors who their consumers are
  for (const auto &element: m_operatorMap) {
	auto ctx = element.second;
	auto op = ctx->op();
	for (const auto &consumerEntry: op->consumers()) {
	  auto consumer = consumerEntry.second;
	  auto entry = LocalOperatorDirectoryEntry(consumer->name(),
											   consumer->actorHandle(),
											   OperatorRelationshipType::Consumer,
											   false);
	  ctx->operatorMap().insert(entry);
	}
  }
}

//void graph::OperatorGraph::write_graph(const std::string &file) {
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
//	std::string nodeName = op.second->op()->name();
//	auto node = agnode(graph, const_cast<char *>(nodeName.c_str()), true);
//
//	agset(node, const_cast<char *>("shape"), const_cast<char *>("plaintext"));
//
//	std::string nodeLabel = "<table border='1' cellborder='0' cellpadding='5'>"
//							"<tr><td align='left'><b>" + op.second->op()->getType() + "</b></td></tr>"
//																					  "<tr><td align='left'>"
//		+ op.second->op()->name() + "</td></tr>"
//									"</table>";
//	char *htmlNodeLabel = agstrdup_html(graph, const_cast<char *>(nodeLabel.c_str()));
//	agset(node, const_cast<char *>("label"), htmlNodeLabel);
//	agstrfree(graph, htmlNodeLabel);
//  }
//
//  // Add all the edges
//  for (const auto &op: this->m_operatorMap) {
//	auto opNode = agfindnode(graph, (char *)(op.second->op()->name().c_str()));
//	for (const auto &c: op.second->op()->consumers()) {
//	  auto consumerOpNode = agfindnode(graph, (char *)(c.second->name().c_str()));
//	  agedge(graph, opNode, consumerOpNode, const_cast<char *>(std::string("Edge").c_str()), true);
//	}
//  }
//
//  const std::experimental::filesystem::path &path = std::experimental::filesystem::path(file);
//  if (!std::experimental::filesystem::exists(path.parent_path())) {
//	throw std::runtime_error("Could not open file '" + file + "' for writing. Parent directory does not exist");
//  } else {
//	FILE *outFile = fopen(file.c_str(), "w");
//	if (outFile == nullptr) {
//	  throw std::runtime_error("Could not open file '" + file + "' for writing. Errno: " + std::to_string(errno));
//	}
//
//	gvLayout(gvc, graph, "dot");
//	gvRender(gvc, graph, "svg", outFile);
//
//	fclose(outFile);
//
//	gvFreeLayout(gvc, graph);
//	agclose(graph);
//	gvFreeContext(gvc);
//  }
//}

tl::expected<long, std::string> graph::OperatorGraph::getElapsedTime() {

  if (startTime_.time_since_epoch().count() == 0)
	return tl::unexpected(std::string("Execution time unavailable, query has not been started"));
  if (stopTime_.time_since_epoch().count() == 0)
	return tl::unexpected(std::string("Execution time unavailable, query has not been stopped"));

  return std::chrono::duration_cast<std::chrono::nanoseconds>(stopTime_ - startTime_).count();
}

std::shared_ptr<Operator> graph::OperatorGraph::getOperator(const std::string &name) {
  return this->m_operatorMap.find(name)->second->op();
}

std::map<std::string, std::shared_ptr<OperatorContext>> graph::OperatorGraph::getOperators() {
  return this->m_operatorMap;
}

std::string graph::OperatorGraph::showMetrics() {

  std::stringstream ss;

  ss << std::endl;

  auto operators = getOperators();

  long totalProcessingTime = 0;
  for (auto &entry : operators) {
	auto processingTime = entry.second->operatorActor()->getProcessingTime();
	totalProcessingTime += processingTime;
  }

  auto totalExecutionTime = getElapsedTime().value();
  std::stringstream formattedExecutionTime;
  formattedExecutionTime << totalExecutionTime << " \u33B1" << " (" << ((double)totalExecutionTime / 1000000000.0)
						 << " secs)";
  ss << std::left << std::setw(60) << "Total Execution Time ";
  ss << std::left << std::setw(60) << formattedExecutionTime.str();
  ss << std::endl;
  ss << std::endl;

  ss << std::left << std::setw(120) << "Operator Execution Times" << std::endl;
  ss << std::setfill(' ');

  ss << std::left << std::setw(120) << std::setfill('-') << "" << std::endl;
  ss << std::setfill(' ');

  ss << std::left << std::setw(60) << "Operator";
  ss << std::left << std::setw(40) << "Execution Time";
  ss << std::left << std::setw(20) << "% Total Time";
  ss << std::endl;

  ss << std::left << std::setw(120) << std::setfill('-') << "" << std::endl;
  ss << std::setfill(' ');

  for (auto &entry : operators) {
	auto operatorName = entry.second->op()->name();
	auto processingTime = entry.second->operatorActor()->getProcessingTime();
	auto processingFraction = (double)processingTime / (double)totalProcessingTime;
	std::stringstream formattedProcessingTime;
	formattedProcessingTime << processingTime << " \u33B1" << " (" << ((double)processingTime / 1000000000.0)
							<< " secs)";
	std::stringstream formattedProcessingPercentage;
	formattedProcessingPercentage << (processingFraction * 100.0);
	ss << std::left << std::setw(60) << operatorName;
	ss << std::left << std::setw(40) << formattedProcessingTime.str();
	ss << std::left << std::setw(20) << formattedProcessingPercentage.str();
	ss << std::endl;
  }

  ss << std::left << std::setw(120) << std::setfill('-') << "" << std::endl;
  ss << std::setfill(' ');

  std::stringstream formattedProcessingTime;
  formattedProcessingTime << totalProcessingTime << " \u33B1" << " (" << ((double)totalProcessingTime / 1000000000.0)
						  << " secs)";
  ss << std::left << std::setw(60) << "Total ";
  ss << std::left << std::setw(40) << formattedProcessingTime.str();
  ss << std::left << std::setw(20) << "100.0";
  ss << std::endl;
  ss << std::endl;

  return ss.str();
}

const long &graph::OperatorGraph::getId() const {
  return id_;
}
