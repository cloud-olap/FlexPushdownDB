//
// Created by matt on 7/7/20.
//

#ifndef NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_GRAPH_OPERATORGRAPH_H
#define NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_GRAPH_OPERATORGRAPH_H

#include <memory>
#include <string>

#include <normal/core/Operator.h>
#include <normal/core/OperatorContext.h>
#include <normal/core/OperatorDirectory.h>
#include <normal/core/cache/SegmentCacheActor.h>
#include <normal/core/OperatorManager.h>

using namespace normal::core;
using namespace normal::core::cache;

namespace normal::core::graph {

// TODO: This eventually should be a globally unique name
inline constexpr const char *GraphRootActorName = "root";

/**
 * Executable graph of operators
 */
class OperatorGraph {

public:
  OperatorGraph(long id, const std::shared_ptr<OperatorManager>& operatorManager);
  static std::shared_ptr<OperatorGraph> make(const std::shared_ptr<OperatorManager>& operatorManager);
  void put(const std::shared_ptr<Operator> &op);
  void start();
  void startOperatorAndProducers(const std::shared_ptr<Operator>& op, std::unordered_map<std::string, bool> operatorStates);
  void join();
  void boot();
  void write_graph(const std::string &file);
  std::shared_ptr<Operator> getOperator(const std::string &);
  std::map<std::string, std::shared_ptr<OperatorContext>> getOperators();
  tl::expected<long, std::string> getElapsedTime();
  std::string showMetrics();
  const long &getId() const;

private:
  long id_;
  std::map<std::string, std::shared_ptr<OperatorContext>> m_operatorMap;
  OperatorDirectory operatorDirectory_;
  std::shared_ptr<OperatorManager> operatorManager_;
  std::shared_ptr<caf::scoped_actor> rootActor_;

  std::chrono::steady_clock::time_point startTime_;
  std::chrono::steady_clock::time_point stopTime_;

};

}

#endif //NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_GRAPH_OPERATORGRAPH_H
