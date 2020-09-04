//
// Created by matt on 26/3/20.
//

#include <normal/sql/Interpreter.h>

#include <normal/sql/NormalSQLLexer.h>
#include <normal/sql/NormalSQLParser.h>
#include <normal/sql/Globals.h>
#include <normal/plan/LogicalPlan.h>
#include <normal/plan/Planner.h>
#include <normal/plan/mode/Modes.h>
#include <normal/cache/LRUCachingPolicy.h>

#include "visitor/Visitor.h"

using namespace normal::sql;

Interpreter::Interpreter() :
  catalogues_(std::make_shared<std::unordered_map<std::string, std::shared_ptr<connector::Catalogue>>>()),
  mode_(normal::plan::operator_::mode::Modes::fullPushdownMode()),
  cachingPolicy_(LRUCachingPolicy::make())
{}

Interpreter::Interpreter(const std::shared_ptr<normal::plan::operator_::mode::Mode> &mode,
                         const std::shared_ptr<CachingPolicy>& cachingPolicy) :
  catalogues_(std::make_shared<std::unordered_map<std::string, std::shared_ptr<connector::Catalogue>>>()),
  mode_(mode),
  cachingPolicy_(cachingPolicy)
{}

void Interpreter::parse(const std::string &sql) {

  SPDLOG_DEBUG("Started");

  std::stringstream ss;
  ss << sql;

  antlr4::ANTLRInputStream input(ss);
  NormalSQLLexer lexer(&input);
  antlr4::CommonTokenStream tokens(&lexer);
  NormalSQLParser parser(&tokens);

  antlr4::tree::ParseTree *tree = parser.parse();
  SPDLOG_DEBUG("Parse Tree:\n{}", tree->toStringTree(true));

  visitor::Visitor visitor(this->catalogues_, this->operatorManager_);
  auto untypedLogicalPlans = tree->accept(&visitor);
  auto logicalPlans = untypedLogicalPlans.as<std::shared_ptr<std::vector<std::shared_ptr<plan::LogicalPlan>>>>();

  // TODO: Perhaps support multiple statements in future
  logicalPlan_ = logicalPlans->at(0);

  // Set mode
  for (auto const &logicalOperator: *logicalPlan_->getOperators()) {
    logicalOperator->setMode(mode_);
  }

  // Create physical plan
  std::shared_ptr<plan::PhysicalPlan> physicalPlan;
  physicalPlan = plan::Planner::generate(*logicalPlan_, mode_);

  SPDLOG_INFO("Total {} physical operators", physicalPlan->getOperators()->size());
  // Add the plan to the operatorGraph
  for(const auto& physicalOperator: *physicalPlan->getOperators()){
    operatorGraph_->put(physicalOperator.second);
  }

  SPDLOG_DEBUG("Finished");
}

void Interpreter::put(const std::shared_ptr<connector::Catalogue> &catalogue) {
  catalogues_->insert(std::pair(catalogue->getName(), catalogue));
}

const std::shared_ptr<normal::core::OperatorManager> &Interpreter::getOperatorManager() const {
  return operatorManager_;
}

const std::shared_ptr<normal::plan::LogicalPlan> &Interpreter::getLogicalPlan() const {
  return logicalPlan_;
}

void Interpreter::clearOperatorGraph() {
  operatorGraph_.reset();
  operatorGraph_ = graph::OperatorGraph::make(operatorManager_);
}

std::shared_ptr<normal::core::graph::OperatorGraph> &Interpreter::getOperatorGraph() {
  return operatorGraph_;
}

void Interpreter::boot() {
  operatorManager_ = std::make_shared<normal::core::OperatorManager>(cachingPolicy_);
  operatorManager_->boot();
  operatorManager_->start();
}

void Interpreter::stop() {
  operatorManager_->stop();
}

std::string Interpreter::showMetrics() {
  double totalExecutionTime = 0;
  for (auto const executionTime: executionTimes) {
    totalExecutionTime += executionTime;
  }
  std::stringstream ss;
  ss << std::endl;
  std::stringstream formattedExecutionTime;
  formattedExecutionTime << totalExecutionTime << " secs";
  ss << std::left << std::setw(60) << "Total Execution Time ";
  ss << std::left << std::setw(60) << formattedExecutionTime.str();
  ss << std::endl;

  size_t totalProcessedBytes = 0, totalReturnedBytes = 0;
  for (auto const &bytesTransferred: bytesTransferred) {
    totalProcessedBytes += bytesTransferred.first;
    totalReturnedBytes += bytesTransferred.second;
  }
  std::stringstream formattedProcessedBytes;
  formattedProcessedBytes << totalProcessedBytes << " B" << " ("
                          << ((double)totalProcessedBytes / 1024.0 / 1024.0 / 1024.0) << " GB)";
  std::stringstream formattedReturnedBytes;
  formattedReturnedBytes << totalReturnedBytes << " B" << " ("
                         << ((double)totalReturnedBytes / 1024.0 / 1024.0 / 1024.0) << " GB)";
  ss << std::left << std::setw(60) << "Total Processed Bytes";
  ss << std::left << std::setw(60) << formattedProcessedBytes.str();
  ss << std::endl;
  ss << std::left << std::setw(60) << "Total Returned Bytes";
  ss << std::left << std::setw(60) << formattedReturnedBytes.str();
  ss << std::endl;
  ss << std::endl;

  ss << std::left << std::setw(120) << "Query Execution Times and Bytes Transferred" << std::endl;
  ss << std::setfill(' ');
  ss << std::left << std::setw(120) << std::setfill('-') << "" << std::endl;
  ss << std::setfill(' ');
  ss << std::left << std::setw(15) << "Query";
  ss << std::left << std::setw(30) << "Execution Time";
  ss << std::left << std::setw(30) << "Processed Bytes";
  ss << std::left << std::setw(30) << "Returned Bytes";
  ss << std::endl;
  ss << std::left << std::setw(120) << std::setfill('-') << "" << std::endl;
  ss << std::setfill(' ');
  for (int qid = 1; qid <= executionTimes.size(); ++qid) {
    std::stringstream formattedProcessingTime1;
    formattedProcessingTime1 << executionTimes[qid - 1] << " secs";
    std::stringstream formattedProcessedBytes1;
    formattedProcessedBytes1 << bytesTransferred[qid - 1].first << " B" << " ("
                             << ((double)bytesTransferred[qid - 1].first / 1024.0 / 1024.0 / 1024.0) << " GB)";
    std::stringstream formattedReturnedBytes1;
    formattedReturnedBytes1 << bytesTransferred[qid - 1].second << " B" << " ("
                            << ((double)bytesTransferred[qid - 1].second / 1024.0 / 1024.0 / 1024.0) << " GB)";
    ss << std::left << std::setw(15) << std::to_string(qid);
    ss << std::left << std::setw(30) << formattedProcessingTime1.str();
    ss << std::left << std::setw(30) << formattedProcessedBytes1.str();
    ss << std::left << std::setw(30) << formattedReturnedBytes1.str();
    ss << std::endl;
  }

  return ss.str();
}

void Interpreter::saveMetrics() {
  executionTimes.emplace_back((double) (operatorGraph_->getElapsedTime().value()) / 1000000000.0);
  bytesTransferred.emplace_back(operatorGraph_->getBytesTransferred());
}

const std::shared_ptr<CachingPolicy> &Interpreter::getCachingPolicy() const {
  return cachingPolicy_;
}


