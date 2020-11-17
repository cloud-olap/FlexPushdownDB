//
// Created by matt on 26/3/20.
//

#ifndef NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_INTERPRETER_H
#define NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_INTERPRETER_H

#include <string>
#include <unordered_map>
#include <memory>

#include <normal/connector/Catalogue.h>
#include <normal/core/OperatorManager.h>
#include <normal/core/graph/OperatorGraph.h>
#include <normal/plan/LogicalPlan.h>
#include <normal/plan/mode/Mode.h>

namespace normal::sql {

class Interpreter{

public:
  Interpreter();
  Interpreter(const std::shared_ptr<normal::plan::operator_::mode::Mode> &mode,
              const std::shared_ptr<CachingPolicy>& cachingPolicy);
  [[nodiscard]] const std::shared_ptr<core::OperatorManager> &getOperatorManager() const;
  [[nodiscard]] std::shared_ptr<core::graph::OperatorGraph> &getOperatorGraph();
  void parse(const std::string& sql);
  void put(const std::shared_ptr<connector::Catalogue> &catalogue);
  const std::shared_ptr<plan::LogicalPlan> &getLogicalPlan() const;
  void clearOperatorGraph();
  void boot();
  void stop();
  void saveMetrics();
  void saveHitRatios();
  void clearMetrics();
  std::string showMetrics();
  std::string showHitRatios();
  const std::shared_ptr<CachingPolicy> &getCachingPolicy() const;

private:
  std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<connector::Catalogue>>> catalogues_;
  std::shared_ptr<plan::LogicalPlan> logicalPlan_;
  std::shared_ptr<core::OperatorManager> operatorManager_;
  std::shared_ptr<core::graph::OperatorGraph> operatorGraph_;
  std::shared_ptr<normal::plan::operator_::mode::Mode> mode_;
  std::shared_ptr<CachingPolicy> cachingPolicy_;

  /*
   * About result metrics
   */
  std::vector<double> executionTimes_;
  std::vector<std::pair<size_t, size_t>> bytesTransferred_;
  std::vector<size_t> numRequests_;
  std::vector<double> hitRatios_;
  std::vector<double> shardHitRatios_;
};

}

#endif //NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_INTERPRETER_H
