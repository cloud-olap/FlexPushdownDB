//
// Created by Yifei Yang on 3/2/21.
//

#include "MathModelTest.h"
#include "ExperimentUtil.h"
#include <normal/sql/Interpreter.h>
#include <normal/plan/mode/Modes.h>
#include <normal/cache/FBRSCachingPolicy.h>
#include <normal/connector/MiniCatalogue.h>
#include <normal/pushdown/filter/Filter.h>
#include <normal/pushdown/Globals.h>

using namespace normal::ssb;
using namespace normal::sql;
using namespace normal::pushdown;
using namespace normal::pushdown::s3;

MathModelTest::MathModelTest(size_t networkLimit, size_t chunkSize, int numRuns) :
  networkLimit_(networkLimit),
  chunkSize_(chunkSize),
  numRuns_(numRuns) {}

std::string showMeasurementMetrics(const double executionTime,
                                   const size_t processedBytes,
                                   const size_t returnedBytes,
                                   const size_t getTransferNS,
                                   const size_t getConvertNS,
                                   const size_t selectTransferNS,
                                   const size_t selectConvertNS,
                                   const double hitRatio,
                                   const std::shared_ptr<normal::plan::operator_::mode::Mode>& mode) {
  // collect metrics
  std::stringstream formattedProcessingTime1;
  formattedProcessingTime1 << executionTime << " secs";

  std::stringstream formattedProcessedBytes1;
  formattedProcessedBytes1 << processedBytes << " B" << " ("
                           << ((double) processedBytes / 1024.0 / 1024.0 / 1024.0) << " GB)";

  std::stringstream formattedReturnedBytes1;
  formattedReturnedBytes1 << returnedBytes << " B" << " ("
                          << ((double) returnedBytes / 1024.0 / 1024.0 / 1024.0) << " GB)";

  std::stringstream formattedGetTransferConvertRate;
  formattedGetTransferConvertRate.precision(4);
  if (getTransferNS > 0 && getConvertNS > 0) {
    formattedGetTransferConvertRate << ((double) returnedBytes / 1024.0 / 1024.0) /
                                       ((double) (getTransferNS + getConvertNS) / 1.0e9) << " MB/s/req";
  } else {
    formattedGetTransferConvertRate << "NA";
  }

  std::stringstream formattedSelectTransferConvertRate;
  formattedSelectTransferConvertRate.precision(4);
  if (selectTransferNS > 0 && selectConvertNS > 0) {
    formattedSelectTransferConvertRate << ((double) returnedBytes / 1024.0 / 1024.0) /
                                          ((double) (selectTransferNS + selectConvertNS) / 1.0e9) << " MB/s/req";
  } else {
    formattedSelectTransferConvertRate << "NA";
  }

  std::stringstream formattedS3SelectSelectivity;
  if (returnedBytes && selectTransferNS > 0 && getTransferNS == 0) {
    formattedS3SelectSelectivity << (double) returnedBytes / (double) processedBytes;
  } else {
    formattedS3SelectSelectivity << "NA";
  }

  std::stringstream formattedHitRatio;
  formattedHitRatio << hitRatio;

  // format metrics
  std::stringstream ss;
  ss << std::left << std::setw(20) << mode->toString();
  ss << std::left << std::setw(18) << formattedProcessingTime1.str();
  ss << std::left << std::setw(30) << formattedProcessedBytes1.str();
  ss << std::left << std::setw(30) << formattedReturnedBytes1.str();
  ss << std::left << std::setw(25) << formattedGetTransferConvertRate.str();
  ss << std::left << std::setw(25) << formattedSelectTransferConvertRate.str();
  ss << std::left << std::setw(22) << formattedS3SelectSelectivity.str();
  ss << std::left << std::setw(15) << formattedHitRatio.str();
  ss << std::endl;

  return ss.str();
}

double measureLocalSpeed(normal::sql::Interpreter& i, std::filesystem::path& sql_file_dir_path) {
  SPDLOG_INFO("Measurement for local bandwidth:");
  filter::recordSpeeds = true;

  i.clearMetrics();
  i.clearHitRatios();
  auto sql_file_path = sql_file_dir_path.append(fmt::format("{}.sql", 2));
  auto sql = read_file(sql_file_path.string());
  executeSql(i, sql, true, false, "");
  sql_file_dir_path = sql_file_dir_path.parent_path();
  SPDLOG_INFO("Query 1 for local bandwidth finished, hit ratio: {}", i.getHitRatios()[0]);
  auto time1 = i.getExecutionTimes()[0];
  auto megaBytesFiltered = (double)filter::totalBytesFiltered_ / 1024.0 / 1024.0;
  filter::totalBytesFiltered_ = 0;

//  i.clearMetrics();
//  i.clearHitRatios();
//  sql_file_path = sql_file_dir_path.append(fmt::format("{}.sql", 3));
//  sql = read_file(sql_file_path.string());
//  executeSql(i, sql, true, false, "");
//  sql_file_dir_path = sql_file_dir_path.parent_path();
//  SPDLOG_INFO("Query 2 for local bandwidth finished, hit ratio: {}", i.getHitRatios()[0]);
//  auto time2 = i.getExecutionTimes()[0];

  filter::recordSpeeds = false;
  return megaBytesFiltered / (time1);
}

void normal::ssb::MathModelTest::runTest() {  // unit: B/s
  spdlog::set_level(spdlog::level::info);
  std::stringstream ss;
  ss << std::setfill(' ');
  ss << std::left << std::setw(180) << std::setfill('-') << "" << std::endl;
  ss << std::setfill(' ');
  ss << std::left << std::setw(20) << "Mode";
  ss << std::left << std::setw(18) << "Execution Time";
  ss << std::left << std::setw(30) << "Processed Bytes";
  ss << std::left << std::setw(30) << "Returned Bytes";
  ss << std::left << std::setw(25) << "GET Transfer+Convert";
  ss << std::left << std::setw(25) << "SELECT Transfer+Convert";
  ss << std::left << std::setw(22) << "% Data S3 Selected";
  ss << std::left << std::setw(15) << "Hit ratio";
  ss << std::endl;
  ss << std::left << std::setw(180) << std::setfill('-') << "" << std::endl;
  ss << std::setfill(' ');

  // set parameters
  if (networkLimit_ > 0) {
    normal::pushdown::NetworkLimit = networkLimit_;
    DefaultS3Client = AWSClient::defaultS3Client();
  }
  if (chunkSize_ > 0) {
    normal::tuple::DefaultChunkSize = chunkSize_;
  }

  normal::connector::defaultMiniCatalogue = normal::connector::MiniCatalogue::defaultMiniCatalogue(
          bucketName_, dirPrefix_);

  // modes
  std::vector<std::shared_ptr<normal::plan::operator_::mode::Mode>> modes;
  modes.emplace_back(normal::plan::operator_::mode::Modes::fullPushdownMode());
  modes.emplace_back(normal::plan::operator_::mode::Modes::pullupCachingMode());
  modes.emplace_back(normal::plan::operator_::mode::Modes::hybridCachingMode());

  // as now we make many S3 requests at the same time, the first batch of requests meet a huge delay (5 secs)
  // so run one pushdown query first
  SPDLOG_INFO("Beginning query to avoid first-run latency:");
  runTestSingleMode(normal::plan::operator_::mode::Modes::fullPushdownMode(), false);

  // test on each mode
  for (int k = 0; k < numRuns_; k++) {
    SPDLOG_INFO("Trial {}:", k + 1);
    for (auto const& mode: modes) {
      runTestSingleMode(mode, true);
    }
  }

  // compute avg for each metrics value and format them
  for (auto const& mode: modes) {
    auto& metricsVec = metricsMap_.find(mode->toString())->second;
    int minId = 0;
    double minExecutionTime = metricsVec.executionTimeVec[0];
    for (int id = 1; id < (int) metricsVec.executionTimeVec.size(); id++) {
      if (metricsVec.executionTimeVec[id] < minExecutionTime) {
        minId = id;
        minExecutionTime = metricsVec.executionTimeVec[id];
      }
    }
//    double avgExecutionTime = std::accumulate(metricsVec.executionTimeVec.begin(), metricsVec.executionTimeVec.end(), 0.0) / metricsVec.executionTimeVec.size();
//    size_t avgProcessedBytes = std::accumulate(metricsVec.processedBytesVec.begin(), metricsVec.processedBytesVec.end(), 0.0) / metricsVec.processedBytesVec.size();
//    size_t avgReturnedBytes = std::accumulate(metricsVec.returnedBytesVec.begin(), metricsVec.returnedBytesVec.end(), 0.0) / metricsVec.returnedBytesVec.size();
//    size_t avgGetTransferNS = std::accumulate(metricsVec.getTransferNSVec.begin(), metricsVec.getTransferNSVec.end(), 0.0) / metricsVec.getTransferNSVec.size();
//    size_t avgGetConvertNS = std::accumulate(metricsVec.getConvertNSVec.begin(), metricsVec.getConvertNSVec.end(), 0.0) / metricsVec.getConvertNSVec.size();
//    size_t avgSelectTransferNS = std::accumulate(metricsVec.selectTransferNSVec.begin(), metricsVec.selectTransferNSVec.end(), 0.0) / metricsVec.selectTransferNSVec.size();
//    size_t avgSelectConvertNS = std::accumulate(metricsVec.selectConvertNSVec.begin(), metricsVec.selectConvertNSVec.end(), 0.0) / metricsVec.selectConvertNSVec.size();
//    double avgHitRatio = std::accumulate(metricsVec.hitRatioVec.begin(), metricsVec.hitRatioVec.end(), 0.0) / metricsVec.hitRatioVec.size();
    ss << showMeasurementMetrics (metricsVec.executionTimeVec[minId],
                                  metricsVec.processedBytesVec[minId],
                                  metricsVec.returnedBytesVec[minId],
                                  metricsVec.getTransferNSVec[minId],
                                  metricsVec.getConvertNSVec[minId],
                                  metricsVec.selectTransferNSVec[minId],
                                  metricsVec.selectConvertNSVec[minId],
                                  metricsVec.hitRatioVec[minId],
                                  mode);
  }

  // format local and network speed
  std::stringstream formattedLocalSpeed;
  double avgLocalSpeed = std::accumulate(localSpeedVec_.begin(), localSpeedVec_.end(), 0.0) / localSpeedVec_.size();
  formattedLocalSpeed << avgLocalSpeed << " MB/s";

  std::stringstream formattedNetworkSpeed;
  if (networkLimit_ > 0)
    formattedNetworkSpeed << (double) (networkLimit_ / 1024.0 / 1024.0) << " MB/s";
  else
    formattedNetworkSpeed << "Unlimited";

  ss << std::left << std::setw(20) << "Local bandwidth:";
  ss << std::left << std::setw(20) << formattedLocalSpeed.str();
  ss << std::endl;
  ss << std::left << std::setw(20) << "Network bandwidth:";
  ss << std::left << std::setw(20) << formattedNetworkSpeed.str();
  ss << std::endl;

  SPDLOG_INFO("Metrics summary:\n{}", ss.str());

  // Output to file
  auto metricsFilePath = std::filesystem::current_path().append("math_model_metrics");
  std::ofstream fout;
  fout.open(metricsFilePath.string(), std::ofstream::out | std::ofstream::app);
  fout << ss.str();
  fout.flush();
  fout.close();
}

void normal::ssb::MathModelTest::runTestSingleMode(
        const std::shared_ptr<normal::plan::operator_::mode::Mode> &mode,
        bool saveMetrics) {
  auto cachingPolicy = FBRSCachingPolicy::make(cacheSize_, mode);  // caching policy doesn't matter here
  auto sql_file_dir_path = std::filesystem::current_path().append("sql/generated");
  normal::sql::Interpreter i(mode, cachingPolicy);
  configureS3ConnectorMultiPartition(i, bucketName_, dirPrefix_);
  SPDLOG_INFO("{} mode:", mode->toString());
  i.boot();

  // query for caching
  if (mode->id() != normal::plan::operator_::mode::ModeId::FullPullup &&
      mode->id() != normal::plan::operator_::mode::ModeId::FullPushdown) {
    SPDLOG_INFO("Query for caching:");
    auto sql_file_path = sql_file_dir_path.append(fmt::format("{}.sql", 1));
    auto sql = read_file(sql_file_path.string());
    executeSql(i, sql, true, false, "");
    sql_file_dir_path = sql_file_dir_path.parent_path();
  }

  normal::cache::allowFetchSegments = false;
  i.clearMetrics();
  i.clearHitRatios();

  // query for measurement
  SPDLOG_INFO("Query for measurement:");
  auto sql_file_path = sql_file_dir_path.append(fmt::format("{}.sql", 2));
  auto sql = read_file(sql_file_path.string());
  executeSql(i, sql, true, false, "");
  sql_file_dir_path = sql_file_dir_path.parent_path();
  SPDLOG_INFO("{} mode finished\nExecution metrics:\n{}", mode->toString(), i.showMetrics());
  SPDLOG_INFO("Cache hit ratios:\n{}", i.showHitRatios());

  // save metrics
  if (saveMetrics) {
    S3SelectScanStats s3SelectScanStats = i.getS3SelectScanStats()[0];
    auto metricsPair = metricsMap_.find(mode->toString());
    if (metricsPair != metricsMap_.end()) {
      auto& metricsVec = metricsPair->second;
      metricsVec.executionTimeVec.emplace_back(i.getExecutionTimes()[0]);
      metricsVec.processedBytesVec.emplace_back(s3SelectScanStats.processedBytes);
      metricsVec.returnedBytesVec.emplace_back(s3SelectScanStats.returnedBytes);
      metricsVec.getTransferNSVec.emplace_back(s3SelectScanStats.getTransferTimeNS);
      metricsVec.getConvertNSVec.emplace_back(s3SelectScanStats.getConvertTimeNS);
      metricsVec.selectTransferNSVec.emplace_back(s3SelectScanStats.selectTransferTimeNS);
      metricsVec.selectConvertNSVec.emplace_back(s3SelectScanStats.selectConvertTimeNS);
      metricsVec.hitRatioVec.emplace_back(i.getHitRatios()[0]);
    } else {
      MetricsVec metricsVec{
              std::vector<double>{i.getExecutionTimes()[0]},
              std::vector<size_t>{s3SelectScanStats.processedBytes},
              std::vector<size_t>{s3SelectScanStats.returnedBytes},
              std::vector<size_t>{s3SelectScanStats.getTransferTimeNS},
              std::vector<size_t>{s3SelectScanStats.getConvertTimeNS},
              std::vector<size_t>{s3SelectScanStats.selectTransferTimeNS},
              std::vector<size_t>{s3SelectScanStats.selectConvertTimeNS},
              std::vector<double>{i.getHitRatios()[0]}
      };
      metricsMap_.emplace(mode->toString(), metricsVec);
    }
  }

  normal::cache::allowFetchSegments = true;

  // query for measuring local bandwidth
  if (mode->id() == normal::plan::operator_::mode::ModeId::PullupCaching) {
    auto localSpeed = measureLocalSpeed(i, sql_file_dir_path);
    localSpeedVec_.emplace_back(localSpeed);
  }

  i.getOperatorGraph().reset();
  i.stop();
}

