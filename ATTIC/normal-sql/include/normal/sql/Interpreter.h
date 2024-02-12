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
#include <normal/plan/Mode.h>

using namespace normal::pushdown::s3;

namespace normal::sql {

class Interpreter{

public:
  Interpreter();
  Interpreter(std::shared_ptr<normal::plan::operator_::mode::Mode> mode,
              std::shared_ptr<CachingPolicy>  cachingPolicy);
  [[nodiscard]] const std::shared_ptr<core::OperatorManager> &getOperatorManager() const;
  [[nodiscard]] std::shared_ptr<core::graph::OperatorGraph> &getOperatorGraph();
  void parse(const std::string& sql);
  void put(const std::shared_ptr<connector::Catalogue> &catalogue);
  void clearOperatorGraph();
  void boot();
  void stop();
  void saveMetrics();
  void saveHitRatios();
  void clearMetrics();
  void clearHitRatios();
  std::string showMetrics();
  std::string showHitRatios();

  [[nodiscard]] const std::shared_ptr<plan::LogicalPlan> &getLogicalPlan() const;
  [[nodiscard]] const std::shared_ptr<CachingPolicy> &getCachingPolicy() const;
  [[nodiscard]] const std::vector<double> &getExecutionTimes() const;
  [[nodiscard]] const std::vector<normal::pushdown::s3::S3SelectScanStats> &getS3SelectScanStats() const;
  [[nodiscard]] const std::vector<double> &getHitRatios() const;
  [[maybe_unused]] [[nodiscard]] const std::vector<double> &getShardHitRatios() const;
  [[maybe_unused]] [[nodiscard]] const std::vector<std::tuple<size_t, size_t, size_t>> &getFilterTimeNsInputOutputBytes() const;

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
  std::vector<S3SelectScanStats> s3SelectScanStats_{};
  std::vector<double> hitRatios_;
  std::vector<double> shardHitRatios_;
  std::vector<std::tuple<size_t, size_t, size_t>> filterTimeNSInputOutputBytes_;
};

}

#endif //NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_INTERPRETER_H
