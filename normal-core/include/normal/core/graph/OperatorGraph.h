//
// Created by matt on 7/7/20.
//

#ifndef NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_GRAPH_OPERATORGRAPH_H
#define NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_GRAPH_OPERATORGRAPH_H

#include <memory>
#include <string>

#include <caf/all.hpp>
#include <tl/expected.hpp>

#include <normal/core/Operator.h>
#include <normal/core/OperatorContext.h>
#include <normal/core/OperatorDirectory.h>
#include <normal/core/OperatorManager.h>
#include <normal/core/Forward.h>
#include <normal/pushdown/collate/Collate.h>
#include <normal/pushdown/collate/Collate2.h>
#include <normal/pushdown/s3/S3SelectScan.h>

using namespace normal::core;
using namespace normal::core::cache;
using namespace normal::pushdown;
using namespace normal::pushdown::s3;
using namespace normal::pushdown::collate;

namespace normal::core::graph {

// TODO: This eventually should be a globally unique name
inline constexpr const char *GraphRootActorName = "root";

/**
 * Executable graph of operators
 */
class OperatorGraph {

public:
  OperatorGraph(long id, const std::shared_ptr<OperatorManager>& operatorManager);
  ~OperatorGraph();
  static std::shared_ptr<OperatorGraph> make(const std::shared_ptr<OperatorManager>& operatorManager);
  void put(const std::shared_ptr<Operator> &def);
  void start();
  void join();
  void boot();
  void close();
  tl::expected<std::shared_ptr<TupleSet2>, std::string> execute();
  void write_graph(const std::string &file);
  std::shared_ptr<Operator> getOperator(const std::string &);
  tl::expected<long, std::string> getElapsedTime();
  S3SelectScanStats getAggregateS3SelectScanStats();
  std::tuple<size_t, size_t, size_t> getFilterTimeNSInputOutputBytes();
  std::string showMetrics(bool showOpTimes = true, bool showScanMetrics = true);
  [[nodiscard]] const long &getId() const;
  std::shared_ptr<TupleSet> getQueryResult() const;

private:
  long id_;
  OperatorDirectory operatorDirectory_;
  std::weak_ptr<OperatorManager> operatorManager_;
  std::shared_ptr<caf::scoped_actor> rootActor_;
  [[maybe_unused]] CollateActor collateActorHandle_;
  std::shared_ptr<Collate> legacyCollateOperator_;

  std::chrono::steady_clock::time_point startTime_;
  std::chrono::steady_clock::time_point stopTime_;
};

}

#endif //NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_GRAPH_OPERATORGRAPH_H
