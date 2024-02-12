//
// Created by matt on 26/3/20.
//

#include <normal/sql/Interpreter.h>
#include <normal/sql/visitor/Visitor.h>
#include <normal/sql/NormalSQLLexer.h>
#include <normal/sql/NormalSQLParser.h>
#include <normal/sql/Globals.h>
#include <normal/plan/LogicalPlan.h>
#include <normal/plan/Planner.h>
#include <normal/plan/mode/Modes.h>
#include <normal/cache/LRUCachingPolicy.h>

#include <utility>

using namespace normal::sql;
using namespace normal::pushdown::s3;

Interpreter::Interpreter() :
  catalogues_(std::make_shared<std::unordered_map<std::string, std::shared_ptr<connector::Catalogue>>>()),
  mode_(normal::plan::operator_::mode::Modes::fullPushdownMode()),
  cachingPolicy_(LRUCachingPolicy::make())
{}

Interpreter::Interpreter(std::shared_ptr<normal::plan::operator_::mode::Mode> mode,
                         std::shared_ptr<CachingPolicy>  cachingPolicy) :
  catalogues_(std::make_shared<std::unordered_map<std::string, std::shared_ptr<connector::Catalogue>>>()),
  mode_(std::move(mode)),
  cachingPolicy_(std::move(cachingPolicy))
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

  // Set mode and queryId
  auto queryId = operatorGraph_->getId();
  for (auto const &logicalOperator: *logicalPlan_->getOperators()) {
    logicalOperator->setMode(mode_);
    logicalOperator->setQueryId(queryId);
  }

  // Create physical plan
  plan::Planner::setQueryId(queryId);
  std::shared_ptr<plan::PhysicalPlan> physicalPlan;
  physicalPlan = plan::Planner::generate(*logicalPlan_, mode_);

  SPDLOG_DEBUG("Total {} physical operators", physicalPlan->getOperators()->size());
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
  for (auto const executionTime: executionTimes_) {
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
  size_t totalGetTransferTimeNS = 0, totalGetConvertTimeNS = 0;
  size_t totalSelectTransferTimeNS = 0, totalSelectConvertTimeNS = 0;
  size_t totalNumRequests = 0;
  size_t arrowOutputBytes = 0;
  for (S3SelectScanStats stats: s3SelectScanStats_) {
    totalProcessedBytes += stats.processedBytes;
    totalReturnedBytes += stats.returnedBytes;
    totalGetTransferTimeNS += stats.getTransferTimeNS;
    totalGetConvertTimeNS += stats.getConvertTimeNS;
    totalSelectTransferTimeNS += stats.selectTransferTimeNS;
    totalSelectConvertTimeNS += stats.selectConvertTimeNS;
    totalNumRequests += stats.numRequests;
    arrowOutputBytes += stats.outputBytes;
  }

  double totalProcessedBytesGiga = ((double)totalProcessedBytes / 1024.0 / 1024.0 / 1024.0);
  double totalReturnedBytesGiga = ((double)totalReturnedBytes / 1024.0 / 1024.0 / 1024.0);
  double totalArrowBytesGiga = ((double)arrowOutputBytes / 1024.0 / 1024.0 / 1024.0);
  std::stringstream formattedProcessedBytes;
  formattedProcessedBytes << totalProcessedBytes << " B" << " ("
                          << totalProcessedBytesGiga << " GB)";
  std::stringstream formattedReturnedBytes;
  formattedReturnedBytes << totalReturnedBytes << " B" << " ("
                         << totalReturnedBytesGiga << " GB)";
  std::stringstream formattedArrowOutputBytes;
  formattedArrowOutputBytes << arrowOutputBytes << " B" << " ("
                            << totalArrowBytesGiga << " GB)";
  std::stringstream formattedStorageFormatToArrowSize;
  if (arrowOutputBytes > 0) {
    formattedStorageFormatToArrowSize << (double) totalReturnedBytes /  (double) arrowOutputBytes << "x";
  } else {
    formattedStorageFormatToArrowSize << "NA";
  }

  std::stringstream formattedAverageGetTransferRate;
  std::stringstream formattedAverageGetConvertRate;
  std::stringstream formattedAverageGetTransferAndConvertRate;
  if (totalGetTransferTimeNS > 0 && totalGetConvertTimeNS > 0) {
    double averageGetTransferRateMBs = ((double)totalReturnedBytes / 1024.0 / 1024.0) / ((double) totalGetTransferTimeNS / 1.0e9);
    double averageGetConvertRateMBs = ((double)totalReturnedBytes / 1024.0 / 1024.0) / ((double) totalGetConvertTimeNS / 1.0e9);
    double averageGetTransferAndConvertRateMBs = ((double)totalReturnedBytes / 1024.0 / 1024.0) /
            (((double) totalGetTransferTimeNS + totalGetConvertTimeNS) / 1.0e9);
    formattedAverageGetTransferRate << averageGetTransferRateMBs << " MB/s/req";
    formattedAverageGetConvertRate << averageGetConvertRateMBs << " MB/s/req";
    formattedAverageGetTransferAndConvertRate << averageGetTransferAndConvertRateMBs << " MB/s/req";
  } else {
    formattedAverageGetTransferRate << "NA";
    formattedAverageGetConvertRate << "NA";
    formattedAverageGetTransferAndConvertRate << "NA";
  }

  std::stringstream formattedAverageSelectTransferRate;
  std::stringstream formattedAverageSelectConvertRate;
  std::stringstream formattedAverageSelectTransferAndConvertRate;
  if (totalSelectTransferTimeNS > 0 && totalSelectConvertTimeNS > 0) {
    // Making the assumption that transfer and convert don't occur at same time, which seems plausible since
    // each request appears to be pinned to one cpu.
    double averageSelectTransferRateMBs = ((double)totalReturnedBytes / 1024.0 / 1024.0) / ((double) (totalSelectTransferTimeNS) / 1.0e9);
    double averageSelectConvertRateMBs = ((double)totalReturnedBytes / 1024.0 / 1024.0) / ((double) totalSelectConvertTimeNS / 1.0e9);
    double averageSelectTransferAndConvertRateMBs = ((double)totalReturnedBytes / 1024.0 / 1024.0) / ((double) (totalSelectTransferTimeNS + totalSelectConvertTimeNS) / 1.0e9);
    formattedAverageSelectTransferRate << averageSelectTransferRateMBs << " MB/s/req";
    formattedAverageSelectConvertRate << averageSelectConvertRateMBs << " MB/s/req";
    formattedAverageSelectTransferAndConvertRate << averageSelectTransferAndConvertRateMBs << " MB/s/req";
  } else {
    formattedAverageSelectTransferRate << "NA";
    formattedAverageSelectConvertRate << "NA";
    formattedAverageSelectTransferAndConvertRate << "NA";
  }

  size_t filterTimeNS = 0, filterInputBytes = 0, filterOutputBytes = 0;
  for (auto const &filterTimeNSInputOutputByte: filterTimeNSInputOutputBytes_) {
    filterTimeNS += std::get<0>(filterTimeNSInputOutputByte);
    filterInputBytes += std::get<1>(filterTimeNSInputOutputByte);
    filterOutputBytes += std::get<2>(filterTimeNSInputOutputByte);
  }
  std::stringstream formattedLocalFilterRateGBs;
  std::stringstream formattedLocalFilterGB;
  std::stringstream formattedLocalFilterSelectivity;
  if (filterTimeNS > 0) {
    double filterGB = ((double)filterInputBytes / 1024.0 / 1024.0 / 1024.0);
    formattedLocalFilterRateGBs << filterGB / ((double)filterTimeNS  / 1.0e9) << " GB/s/req";
    formattedLocalFilterGB << filterGB << " GB";
    formattedLocalFilterSelectivity << ((double) filterOutputBytes / (double) filterInputBytes) * 100 << "%";
  } else {
    formattedLocalFilterRateGBs << "NA";
    formattedLocalFilterGB << "NA";
    formattedLocalFilterSelectivity << "NA";
  }

  // Cost of c5a.8xlarge instance in US West (North California)
  // All other costs are for the region US West (North California) as well
//  double ec2Price = 1.52, totalCost = 0;
  // Cost of c5n.9xlarge instance in US West (North California)
  // All other costs are for the region US West (North California) as well
  double ec2Price = 2.43, totalCost = 0;
//  // Cost of c5a.16xlarge instance in US West (North California)
//  // All other costs are for the region US West (North California) as well
//  double ec2Price = 3.04, totalCost = 0;
  double getRequestCost = 0.0;
  double s3ScanCost = 0.0;
  double s3ReturnCost = 0.0;
  double runtimeCost = 0.0;
  if (mode_->id() == normal::plan::operator_::mode::ModeId::FullPullup ||
      mode_->id() == normal::plan::operator_::mode::ModeId::PullupCaching) {
    getRequestCost = ((double) totalNumRequests) * 0.00000044;  // GET request cost
    runtimeCost = totalExecutionTime / 3600 * ec2Price;         // runtime cost
    totalCost = getRequestCost + runtimeCost;
  } else {
    getRequestCost = ((double) totalNumRequests) * 0.00000044;  // GET request cost
    s3ScanCost = totalProcessedBytesGiga * 0.0022;              // s3 scan cost
    s3ReturnCost = totalReturnedBytesGiga * 0.0008;             // s3 return cost
    runtimeCost = totalExecutionTime / 3600 * ec2Price;         // runtime cost
    totalCost = getRequestCost + s3ScanCost + s3ReturnCost + runtimeCost;
  }
  std::stringstream formattedCost;
  formattedCost << totalCost << " $";
  std::stringstream formattedGetRequestCost;
  formattedGetRequestCost << getRequestCost << " $";
  std::stringstream formattedS3ScanCost;
  formattedS3ScanCost << s3ScanCost << " $";
  std::stringstream formattedS3ReturnCost;
  formattedS3ReturnCost << s3ReturnCost << " $";
  std::stringstream formattedRuntimeCost;
  formattedRuntimeCost << runtimeCost << " $";

  ss << std::left << std::setw(60) << "Total Processed Bytes";
  ss << std::left << std::setw(60) << formattedProcessedBytes.str();
  ss << std::endl;
  ss << std::left << std::setw(60) << "Total Returned Bytes";
  ss << std::left << std::setw(60) << formattedReturnedBytes.str();
  ss << std::endl;
  ss << std::left << std::setw(60) << "Total Arrow Converted Bytes";
  ss << std::left << std::setw(60) << formattedArrowOutputBytes.str();
  ss << std::endl;
  ss << std::left << std::setw(60) << "Total Storage/Compute Data Ratio";
  ss << std::left << std::setw(60) << formattedStorageFormatToArrowSize.str();
  ss << std::endl;
  ss << std::left << std::setw(60) << "Average GET Transfer Rate";
  ss << std::left << std::setw(60) << formattedAverageGetTransferRate.str();
  ss << std::endl;
  ss << std::left << std::setw(60) << "Average GET Convert Rate";
  ss << std::left << std::setw(60) << formattedAverageGetConvertRate.str();
  ss << std::endl;
  ss << std::left << std::setw(60) << "Average GET Transfer and Convert Rate";
  ss << std::left << std::setw(60) << formattedAverageGetTransferAndConvertRate.str();
  ss << std::endl;
  ss << std::left << std::setw(60) << "Average Select Transfer Rate";
  ss << std::left << std::setw(60) << formattedAverageSelectTransferRate.str();
  ss << std::endl;
  ss << std::left << std::setw(60) << "Average Select Convert Rate";
  ss << std::left << std::setw(60) << formattedAverageSelectConvertRate.str();
  ss << std::endl;
  ss << std::left << std::setw(60) << "Average Select Transfer And Convert Rate";
  ss << std::left << std::setw(60) << formattedAverageSelectTransferAndConvertRate.str();
  ss << std::endl;
  ss << std::left << std::setw(60) << "Local filter rate";
  ss << std::left << std::setw(60) << formattedLocalFilterRateGBs.str();
  ss << std::endl;
  ss << std::left << std::setw(60) << "Local filter bytes";
  ss << std::left << std::setw(60) << formattedLocalFilterGB.str();
  ss << std::endl;
  ss << std::left << std::setw(60) << "Local filter selectivity (bytes)";
  ss << std::left << std::setw(60) << formattedLocalFilterSelectivity.str();
  ss << std::endl;
  ss << std::left << std::setw(60) << "Total Request amount";
  ss << std::left << std::setw(60) << totalNumRequests;
  ss << std::endl;
  ss << std::left << std::setw(60) << "Total Cost";
  ss << std::left << std::setw(60) << formattedCost.str();
  ss << std::endl;
  ss << std::left << std::setw(60) << "Total GET Request Cost";
  ss << std::left << std::setw(60) << formattedGetRequestCost.str();
  ss << std::endl;
  ss << std::left << std::setw(60) << "Total S3 Scan Cost";
  ss << std::left << std::setw(60) << formattedS3ScanCost.str();
  ss << std::endl;
  ss << std::left << std::setw(60) << "Total S3 Return Cost";
  ss << std::left << std::setw(60) << formattedS3ReturnCost.str();
  ss << std::endl;
  ss << std::left << std::setw(60) << "Total Runtime Cost";
  ss << std::left << std::setw(60) << formattedRuntimeCost.str();
  ss << std::endl;
  ss << std::endl;

  ss << std::left << std::setw(120) << "Query Execution Times and Bytes Transferred" << std::endl;
  ss << std::setfill(' ');
  ss << std::left << std::setw(155) << std::setfill('-') << "" << std::endl;
  ss << std::setfill(' ');
  ss << std::left << std::setw(8) << "Query";
  ss << std::left << std::setw(18) << "Execution Time";
  ss << std::left << std::setw(30) << "Processed Bytes";
  ss << std::left << std::setw(30) << "Returned Bytes";
  ss << std::left << std::setw(20) << "GET Tran+Conv";
  ss << std::left << std::setw(20) << "Select Tran+Conv";
  ss << std::left << std::setw(10) << "S3 Selectivity";
  ss << std::endl;
  ss << std::left << std::setw(155) << std::setfill('-') << "" << std::endl;
  ss << std::setfill(' ');
  for (size_t qid = 1; qid <= executionTimes_.size(); ++qid) {
    S3SelectScanStats stats = s3SelectScanStats_[qid - 1];
    std::stringstream formattedProcessingTime1;
    formattedProcessingTime1 << executionTimes_[qid - 1] << " secs";
    std::stringstream formattedProcessedBytes1;
    formattedProcessedBytes1 << stats.processedBytes << " B" << " ("
                             << ((double)stats.processedBytes / 1024.0 / 1024.0 / 1024.0) << " GB)";
    std::stringstream formattedReturnedBytes1;
    formattedReturnedBytes1 << stats.returnedBytes << " B" << " ("
                            << ((double)stats.returnedBytes / 1024.0 / 1024.0 / 1024.0) << " GB)";

    std::stringstream formattedGetTransferConvertRate;
    formattedGetTransferConvertRate.precision(4);
    if (stats.getTransferTimeNS > 0 && stats.getConvertTimeNS > 0) {
      formattedGetTransferConvertRate << ((double) stats.returnedBytes / 1024.0 / 1024.0) /
                                 (((double) (stats.getTransferTimeNS + stats.getConvertTimeNS)) / 1.0e9) << " MB/s/req";
    } else {
      formattedGetTransferConvertRate << "NA";
    }

    std::stringstream formattedSelectTransferConvertRate;
    formattedSelectTransferConvertRate.precision(4);
    if (stats.selectTransferTimeNS > 0 && stats.selectConvertTimeNS > 0) {
      formattedSelectTransferConvertRate << ((double) stats.returnedBytes / 1024.0 / 1024.0) /
                                     ((double) (stats.selectTransferTimeNS + stats.selectConvertTimeNS) / 1.0e9) << " MB/s/req";
    } else {
      formattedSelectTransferConvertRate << "NA";
    }
    // FIXME: This only works if the query is entirely pushdown, as the bytes transferred is grouped together for
    //        select and get requests, so they are not differentiated
    std::stringstream formattedS3SelectSelectivity;
    if (stats.returnedBytes > 0 && stats.processedBytes &&
        stats.selectTransferTimeNS > 0 && stats.getConvertTimeNS == 0) {
      formattedS3SelectSelectivity << ((double) stats.returnedBytes / (double) stats.processedBytes) * 100.0 << "%";
    } else {
      formattedS3SelectSelectivity << "NA";
    }
    ss << std::left << std::setw(8) << std::to_string(qid);
    ss << std::left << std::setw(18) << formattedProcessingTime1.str();
    ss << std::left << std::setw(30) << formattedProcessedBytes1.str();
    ss << std::left << std::setw(30) << formattedReturnedBytes1.str();
    ss << std::left << std::setw(20) << formattedGetTransferConvertRate.str();
    ss << std::left << std::setw(20) << formattedSelectTransferConvertRate.str();
    ss << std::left << std::setw(22) << formattedS3SelectSelectivity.str();
    ss << std::endl;
  }

  ss << std::endl;
  ss << std::left << std::setw(120) << "Pushdown and Local filter stats" << std::endl;
  ss << std::setfill(' ');
  ss << std::left << std::setw(155) << std::setfill('-') << "" << std::endl;
  ss << std::setfill(' ');
  ss << std::left << std::setw(8) << "Query";
  ss << std::left << std::setw(15) << "Returned GB";
  ss << std::left << std::setw(12) << "Return %";
  ss << std::left << std::setw(30) << "Storage/Compute Data Ratio";
  ss << std::left << std::setw(25) << "GB Filtered Locally";
  ss << std::left << std::setw(25) << "Local Filter Speed/req";
  ss << std::left << std::setw(17) << "Local Filter %";
  ss << std::left << std::setw(20) << "Conv Output Rate";
  ss << std::endl;
  ss << std::left << std::setw(155) << std::setfill('-') << "" << std::endl;
  ss << std::setfill(' ');
  for (size_t qid = 1; qid <= executionTimes_.size(); ++qid) {
    S3SelectScanStats stats = s3SelectScanStats_[qid - 1];
    std::stringstream formattedReturnedGB;
    std::stringstream formattedReturnedPercentage;

    double processedGB = ((double)stats.processedBytes / 1024.0 / 1024.0 / 1024.0);
    double returnedGB = ((double)stats.returnedBytes / 1024.0 / 1024.0 / 1024.0);
    formattedReturnedGB << returnedGB << " GB";
    if (processedGB > 0) {
      formattedReturnedPercentage << (returnedGB / processedGB) * 100.0 << "%";
    } else {
      formattedReturnedPercentage << "NA";
    }

    double arrowOutputMB = ((double)stats.outputBytes / 1024.0 / 1024.0);
    double arrowOutputGB = ((double)stats.outputBytes / 1024.0 / 1024.0 / 1024.0);
    std::stringstream formattedStorageFormatToArrowSizeX;
    std::stringstream formattedConversionOutputRate;
    if (arrowOutputGB > 0) {
      formattedStorageFormatToArrowSizeX << returnedGB / arrowOutputGB << "x";
      formattedConversionOutputRate << (double)arrowOutputMB /
              ((double)(stats.getConvertTimeNS + stats.selectConvertTimeNS) / 1.0e9) << " MB/s/req";
    } else {
      formattedStorageFormatToArrowSizeX << "NA";
      formattedConversionOutputRate << "NA";
    }

    auto filterTimeNSInputOutputByte = filterTimeNSInputOutputBytes_[qid - 1];
    double filterTimeSec = (double) std::get<0>(filterTimeNSInputOutputByte) / 1.0e9;
    double filterInputGB = (double) std::get<1>(filterTimeNSInputOutputByte) / 1024.0 / 1024.0 / 1024.0;
    double filterOutputGB = (double) std::get<2>(filterTimeNSInputOutputByte) / 1024.0 / 1024.0 / 1024.0;

    std::stringstream formattedLocalFilteredGB;
    std::stringstream formattedLocalFilterSpeed;
    std::stringstream formattedLocalFilterPercent;
    if (filterInputGB > 0) {
      formattedLocalFilteredGB << filterInputGB << " GB";
      formattedLocalFilterSpeed << filterInputGB / filterTimeSec << " GB/s";
      formattedLocalFilterPercent << (filterOutputGB / filterInputGB) * 100.0 << "%";
    } else {
      formattedLocalFilteredGB << "NA";
      formattedLocalFilterSpeed << "NA";
      formattedLocalFilterPercent << "NA";
    }


    ss << std::left << std::setw(8) << std::to_string(qid);
    ss << std::left << std::setw(15) << formattedReturnedGB.str();
    ss << std::left << std::setw(12) << formattedReturnedPercentage.str();
    ss << std::left << std::setw(30) << formattedStorageFormatToArrowSizeX.str();
    ss << std::left << std::setw(25) << formattedLocalFilteredGB.str();
    ss << std::left << std::setw(25) << formattedLocalFilterSpeed.str();
    ss << std::left << std::setw(17) << formattedLocalFilterPercent.str();
    ss << std::left << std::setw(20) << formattedConversionOutputRate.str();
    ss << std::endl;
  }

  return ss.str();
}

void Interpreter::saveMetrics() {
  executionTimes_.emplace_back((double) (operatorGraph_->getElapsedTime().value()) / 1000000000.0);
  s3SelectScanStats_.emplace_back(operatorGraph_->getAggregateS3SelectScanStats());
  filterTimeNSInputOutputBytes_.emplace_back(operatorGraph_->getFilterTimeNSInputOutputBytes());
}

void Interpreter::clearMetrics() {
  executionTimes_.clear();
  s3SelectScanStats_.clear();
  filterTimeNSInputOutputBytes_.clear();
}

void Interpreter::clearHitRatios() {
  hitRatios_.clear();
  shardHitRatios_.clear();
}

const std::shared_ptr<CachingPolicy> &Interpreter::getCachingPolicy() const {
  return cachingPolicy_;
}

std::string Interpreter::showHitRatios() {
  std::stringstream ss;
  ss << std::endl;
  ss << "Hit ratios, Shard Hit Ratios:" << std::endl;
  ss << std::endl;
  for (size_t i = 0; i < hitRatios_.size(); i++) {
    auto qId = i + 1;
    auto const hitRatio = hitRatios_.at(i);
    auto const shardHitRatio = shardHitRatios_.at(i);
    ss << std::left << std::setw(20) << qId;
    ss << std::left << std::setw(30) << hitRatio;
    ss << std::left << std::setw(30) << shardHitRatio;
    ss << std::endl;
  }
  return ss.str();
}

void Interpreter::saveHitRatios() {
  hitRatios_.emplace_back(operatorManager_->getCrtQueryHitRatio());
  shardHitRatios_.emplace_back(operatorManager_->getCrtQueryShardHitRatio());
}

const std::vector<double> &Interpreter::getExecutionTimes() const {
  return executionTimes_;
}

const std::vector<S3SelectScanStats> &Interpreter::getS3SelectScanStats() const {
  return s3SelectScanStats_;
}

const std::vector<double> &Interpreter::getHitRatios() const {
  return hitRatios_;
}

[[maybe_unused]] const std::vector<double> &Interpreter::getShardHitRatios() const {
  return shardHitRatios_;
}

[[maybe_unused]] const std::vector<std::tuple<size_t, size_t, size_t>> &Interpreter::getFilterTimeNsInputOutputBytes() const {
  return filterTimeNSInputOutputBytes_;
}

