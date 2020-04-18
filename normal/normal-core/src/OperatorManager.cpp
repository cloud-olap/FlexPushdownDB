//
// Created by matt on 5/12/19.
//

#include "normal/core/OperatorManager.h"

#include <cassert>
#include <vector>
//#include <filesystem>
#include <experimental/filesystem>

#include <caf/all.hpp>
#include <caf/io/all.hpp>
#include <graphviz/gvc.h>

#include <normal/core/Actors.h>
#include "normal/core/Globals.h"
#include "normal/core/message/Envelope.h"
#include "normal/core/Operator.h"
#include "normal/core/OperatorContext.h"
#include "normal/core/OperatorActor.h"
#include "normal/core/message/StartMessage.h"
#include "normal/core/OperatorDirectory.h"

namespace normal::core {

void OperatorManager::put(const std::shared_ptr<normal::core::Operator> &op) {

  assert(op);

  caf::actor rootActorHandle = normal::core::Actors::toActorHandle(this->rootActor_);

  auto ctx = std::make_shared<normal::core::OperatorContext>(op, rootActorHandle);
  m_operatorMap.insert(std::pair(op->name(), ctx));

  operatorDirectory_.insert(normal::core::OperatorDirectoryEntry(op->name(), std::optional<caf::actor>(), false));
}

void OperatorManager::start() {

  startTime_ = std::chrono::steady_clock::now();

  // Mark all the operators as incomplete
  operatorDirectory_.setIncomplete();


//   Send start messages to the actors
  for (const auto &element: m_operatorMap) {
    auto ctx = element.second;
    auto op = ctx->op();

    std::vector<caf::actor> actorHandles;
    for (const auto &consumer: op->consumers())
      actorHandles.emplace_back(consumer.second->actorHandle());

    auto sm = std::make_shared<message::StartMessage>(actorHandles, "root");

    (*rootActor_)->send(op->actorHandle(), normal::core::message::Envelope(sm));
  }
}

void OperatorManager::stop() {
  // TODO: Send actors a shutdown message
  this->actorSystem->await_actors_before_shutdown(false);

  stopTime_ = std::chrono::steady_clock::now();
}

OperatorManager::OperatorManager() {
  actorSystemConfig.load<caf::io::middleman>();
  actorSystem = std::make_unique<caf::actor_system>(actorSystemConfig);
  rootActor_ = std::make_shared<caf::scoped_actor>(*actorSystem);
}

void OperatorManager::join() {

  SPDLOG_DEBUG("Waiting for all operators to complete");

  auto handle_err = [&](const caf::error &err) {
    aout(*rootActor_) << "AUT (actor under test) failed: "
                      << (*rootActor_)->system().render(err) << std::endl;
  };

  bool allComplete = false;
  (*rootActor_)->receive_while([&] { return !allComplete; })(
      [&](const normal::core::message::Envelope &msg) {
        SPDLOG_DEBUG("Message received  |  actor: 'OperatorManager', messageKind: '{}', from: '{}'",
                     msg.message().type(), msg.message().sender());

        this->operatorDirectory_.setComplete(msg.message().sender());

        allComplete = this->operatorDirectory_.allComplete();

        SPDLOG_DEBUG(this->operatorDirectory_.showString());
        SPDLOG_DEBUG(allComplete);
      },
      handle_err);
}

void OperatorManager::boot() {
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
    caf::actor actorHandle = actorSystem->spawn<normal::core::OperatorActor>(op);
    op->actorHandle(actorHandle);
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

void OperatorManager::write_graph(const std::string &file) {

  auto gvc = gvContext();

  auto graph = agopen(const_cast<char *>(std::string("Execution Plan").c_str()), Agstrictdirected, 0);

  // Init attributes
  agattr(graph, AGNODE, const_cast<char *>("fixedsize"), const_cast<char *>("false"));
  agattr(graph, AGNODE, const_cast<char *>("shape"), const_cast<char *>("ellipse"));
  agattr(graph, AGNODE, const_cast<char *>("label"), const_cast<char *>("<not set>"));
  agattr(graph, AGNODE, const_cast<char *>("fontname"), const_cast<char *>("Arial"));
  agattr(graph, AGNODE, const_cast<char *>("fontsize"), const_cast<char *>("8"));

  // Add all the nodes
  for (const auto &op: this->m_operatorMap) {
    std::string nodeName = op.second->op()->name();
    auto node = agnode(graph, const_cast<char *>(nodeName.c_str()), true);

    agset(node, const_cast<char *>("shape"), const_cast<char *>("plaintext"));

    std::string nodeLabel = "<table border='1' cellborder='0' cellpadding='5'>"
                            "<tr><td align='left'><b>" + op.second->op()->getType() + "</b></td></tr>"
                            "<tr><td align='left'>" + op.second->op()->name() + "</td></tr>"
                            "</table>";
    char *htmlNodeLabel = agstrdup_html(graph, const_cast<char *>(nodeLabel.c_str()));
    agset(node, const_cast<char *>("label"), htmlNodeLabel);
    agstrfree(graph, htmlNodeLabel);
  }

  // Add all the edges
  for (const auto &op: this->m_operatorMap) {
    auto opNode = agfindnode(graph, (char *) (op.second->op()->name().c_str()));
    for (const auto &c: op.second->op()->consumers()) {
      auto consumerOpNode = agfindnode(graph, (char *) (c.second->name().c_str()));
      agedge(graph, opNode, consumerOpNode, const_cast<char *>(std::string("Edge").c_str()), true);
    }
  }

  const std::experimental::filesystem::path &path = std::experimental::filesystem::path(file);
  if (!std::experimental::filesystem::exists(path.parent_path())) {
    throw std::runtime_error("Could not open file '" + file + "' for writing. Parent directory does not exist");
  } else {
    FILE *outFile = fopen(file.c_str(), "w");
    if (outFile == nullptr) {
      throw std::runtime_error("Could not open file '" + file + "' for writing. Errno: " + std::to_string(errno));
    }

    gvLayout(gvc, graph, "dot");
    gvRender(gvc, graph, "svg", outFile);

    fclose(outFile);

    gvFreeLayout(gvc, graph);
    agclose(graph);
    gvFreeContext(gvc);
  }
}

std::shared_ptr<Operator> OperatorManager::getOperator(const std::string &name) {
  return this->m_operatorMap.find(name)->second->op();
}

std::map<std::string, std::shared_ptr<OperatorContext>> OperatorManager::getOperators() {
  return this->m_operatorMap;
}

tl::expected<long, std::string> OperatorManager::getElapsedTime() {

  if(startTime_.time_since_epoch().count() == 0)
    return tl::unexpected(std::string("Execution time unavailable, query has not been started"));
  if(stopTime_.time_since_epoch().count() == 0)
	return tl::unexpected(std::string("Execution time unavailable, query has not been stopped"));

  return std::chrono::duration_cast<std::chrono::nanoseconds>(stopTime_ - startTime_).count();
}

}