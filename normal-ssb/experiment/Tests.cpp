//
// Created by Yifei Yang on 7/7/20.
//

#include "ExperimentUtil.h"
#include "Tests.h"
#include <doctest/doctest.h>
#include <normal/sql/Interpreter.h>
#include <normal/ssb/SSBSchema.h>
#include <normal/ssb/SqlGenerator.h>
#include <normal/pushdown/Globals.h>
#include <normal/pushdown/collate/Collate.h>
#include <normal/plan/mode/Modes.h>
#include <normal/cache/Globals.h>
#include <normal/cache/LRUCachingPolicy.h>
#include <normal/cache/FBRCachingPolicy.h>
#include <normal/cache/FBRSCachingPolicy.h>
#include <normal/cache/WFBRCachingPolicy.h>
#include <normal/cache/BeladyCachingPolicy.h>
#include <normal/connector/s3/S3SelectConnector.h>
#include <normal/connector/MiniCatalogue.h>

#include <aws/s3/model/GetObjectRequest.h>                  // for GetObj...
#include <aws/s3/S3Client.h>
#include <aws/core/utils/memory/stl/AWSString.h>
#include <aws/s3/model/SelectObjectContentRequest.h>
#include <aws/s3/model/CSVInput.h>                          // for CSVInput
#include <aws/s3/model/CSVOutput.h>                         // for CSVOutput
#include <aws/s3/model/ExpressionType.h>                    // for Expressio...
#include <aws/s3/model/FileHeaderInfo.h>                    // for FileHeade...
#include <aws/s3/model/InputSerialization.h>                // for InputSeri...
#include <aws/s3/model/OutputSerialization.h>               // for OutputSer...
#include <aws/s3/model/RecordsEvent.h>                      // for RecordsEvent
#include <aws/s3/model/SelectObjectContentHandler.h>        // for SelectObj...
#include <aws/s3/model/StatsEvent.h>                        // for StatsEvent
#include <aws/s3/model/ListObjectsRequest.h>

#include <arrow/csv/reader.h>                               // for TableReader
#include <arrow/type_fwd.h>                                 // for default_m...

#ifdef __AVX2__
#include <normal/tuple/arrow/CSVToArrowSIMDStreamParser.h>
#include <normal/tuple/arrow/CSVToArrowSIMDChunkParser.h>
#endif

#define SKIP_SUITE false

using namespace normal::ssb;
using namespace normal::pushdown;
using namespace normal::pushdown::collate;
using namespace normal::pushdown::s3;

std::mutex selectBytesReceivedLock;
volatile size_t selectReceivedBytes = 0;
void simpleSelectRequest(const std::shared_ptr<Aws::S3::S3Client>& s3Client, int index) {
  size_t bytesReceived = 0;
  Aws::S3::Model::SelectObjectContentRequest selectObjectContentRequest;
//  std::string bucketName = "demo-bucket";
//  std::string keyName = "data.csv";
//  std::string sql = "SELECT col2, col5, col9, col13, col29, col61, col91 FROM s3object WHERE cast(col1 as int) = 0;";
  std::string bucketName = "flexpushdowndb";
  std::string keyName = fmt::format("ssb-sf100-sortlineorder/csv_150MB/lineorder_sharded/lineorder.tbl.{}", index);
//  std::string keyName = fmt::format("ssb-sf100-sortlineorder/parquet_150MB/lineorder_sharded/lineorder.parquet.{}", index);
//  std::string keyName = fmt::format("ssb-sf100-sortlineorder/parquet_150MB/lineorder_sharded/lineorder.parquet.{}", index);
//  std::string sql = "select lo_revenue, lo_supplycost, lo_orderdate, lo_suppkey, lo_custkey from s3Object";
//  std::string sql = "select lo_revenue, lo_supplycost, lo_orderdate, lo_suppkey, lo_custkey from s3Object "
//                    "where cast(lo_quantity as int) <= 10";
//  std::string sql = "select lo_discount from s3Object";
  std::string sql = "select lo_custkey, lo_orderdate, lo_partkey, lo_revenue, lo_suppkey, lo_supplycost from s3Object where (cast(lo_quantity as int) >= 20 and cast(lo_quantity as int) <= 30)";

  selectObjectContentRequest.SetBucket(Aws::String(bucketName));
  selectObjectContentRequest.SetKey(Aws::String(keyName));

  selectObjectContentRequest.SetExpressionType(Aws::S3::Model::ExpressionType::SQL);
  selectObjectContentRequest.SetExpression(sql.c_str());

  Aws::S3::Model::InputSerialization inputSerialization;

  // CSV input
  Aws::S3::Model::CSVInput csvInput;
  csvInput.SetFileHeaderInfo(Aws::S3::Model::FileHeaderInfo::USE);
  // This is the standard field delimiter and record delimiter for S3 Select, so it is hardcoded here
  csvInput.SetFieldDelimiter("|");
  csvInput.SetRecordDelimiter("\n");
  inputSerialization.SetCSV(csvInput);

//  // Parquet input
//  Aws::S3::Model::ParquetInput parquetInput;
//  inputSerialization.SetParquet(parquetInput);

  selectObjectContentRequest.SetInputSerialization(inputSerialization);

  Aws::S3::Model::CSVOutput csvOutput;
  Aws::S3::Model::OutputSerialization outputSerialization;
  outputSerialization.SetCSV(csvOutput);
  selectObjectContentRequest.SetOutputSerialization(outputSerialization);

  auto schema = SSBSchema::lineOrder();
  auto fields = {
          schema->GetFieldByName("lo_custkey"),
          schema->GetFieldByName("lo_orderdate"),
          schema->GetFieldByName("lo_partkey"),
          schema->GetFieldByName("lo_revenue"),
          schema->GetFieldByName("lo_suppkey"),
          schema->GetFieldByName("lo_supplycost")
  };
  auto inputSchema = std::make_shared<::arrow::Schema>(fields);
  auto outputSchema = std::make_shared<::arrow::Schema>(fields);

  std::string callerName = "testCaller";
  // Only worrying about parser performance when AVX instructions are on as that is the test setup we run in
  // so only added support for that here rather than adding non AVX converting too
#ifdef __AVX2__
    auto parser = std::make_shared<CSVToArrowSIMDChunkParser>(callerName, 16 * 1024 * 1024, inputSchema, outputSchema, normal::connector::defaultMiniCatalogue->getCSVFileDelimiter());
#endif
  std::mutex convertSelectResponseLock;
  std::vector<char*> allocations;
  std::vector<size_t> allocation_sizes;

  Aws::S3::Model::SelectObjectContentHandler handler;

  handler.SetRecordsEventCallback([&](const Aws::S3::Model::RecordsEvent &recordsEvent) {
    SPDLOG_DEBUG("S3 Select RecordsEvent of query {} | size: {}",
                 index,
                 recordsEvent.GetPayload().size());
    auto payload = recordsEvent.GetPayload();
    if (!payload.empty()) {
#ifdef __AVX2__
      parser->parseChunk(reinterpret_cast<char *>(payload.data()), payload.size());
      bytesReceived += payload.size();
#endif
    }
  });
  handler.SetStatsEventCallback([&](const Aws::S3::Model::StatsEvent &statsEvent) {
	SPDLOG_DEBUG("S3 Select StatsEvent  | scanned: {}, processed: {}, returned: {}",
				 statsEvent.GetDetails().GetBytesScanned(),
				 statsEvent.GetDetails().GetBytesProcessed(),
				 statsEvent.GetDetails().GetBytesReturned());
  });
  handler.SetEndEventCallback([&]() {
      SPDLOG_INFO("S3 Select done: {}", index);
  });
  handler.SetOnErrorCallback([&](const Aws::Client::AWSError<Aws::S3::S3Errors> &errors) {
      SPDLOG_INFO("S3 Select Error of query {} | message: {}",
                  index,
                  std::string(errors.GetMessage()));
  });

  selectObjectContentRequest.SetEventStreamHandler(handler);
  uint64_t retrySleepTimeMS = 10;
  while (true) {
    // create a new parser to use as the current one has results from the previous request
    if (parser->isInitialized()) {
      parser = std::make_shared<CSVToArrowSIMDChunkParser>(callerName, 16 * 1024 * 1024, inputSchema, outputSchema, normal::connector::defaultMiniCatalogue->getCSVFileDelimiter());
    }
//  std::chrono::steady_clock::time_point startTransferConvertTime = std::chrono::steady_clock::now();
//  SPDLOG_INFO("Starting select request for {}/{}", bucketName, keyName);
//  auto selectObjectContentOutcome = DefaultS3Client->SelectObjectContent(selectObjectContentRequest);
    auto selectObjectContentOutcome = s3Client->SelectObjectContent(selectObjectContentRequest);

    if (selectObjectContentOutcome.IsSuccess()) {
#ifdef __AVX2__
      auto tupleSet = parser->outputCompletedTupleSet();
      SPDLOG_INFO("Query: {}, Output rows: {}, bytes received: {}", index, tupleSet->numRows(), bytesReceived);
#endif
      selectBytesReceivedLock.lock();
      selectReceivedBytes += bytesReceived;
      selectBytesReceivedLock.unlock();
      break;
    }
//    SPDLOG_INFO("Finished select request for {}/{}", bucketName, keyName);
//  if (selectObjectContentOutcome.IsSuccess()) {
//    SPDLOG_INFO("Select request for {}/{} was a success!", bucketName, keyName);
//  } else {
//    SPDLOG_INFO("Select request for {}/{} was a failure, error= ", bucketName, keyName, selectObjectContentOutcome.GetError().GetMessage());
//  }
    std::this_thread::sleep_for (std::chrono::milliseconds(retrySleepTimeMS));
  }
}

std::mutex getConvertLock;
volatile int activeConvertGets = 0;
std::mutex getBytesReceivedLock;
volatile size_t receivedBytes = 0;
uint64_t simpleGetRequest(int requestNum) {
  bool parsingComplete = false;
  uint64_t retrySleepTimeMS = 1;
  // Do this so that it is hopefully easier for requests that are done
  // to acquire the lock and decrement activeConvertGets due to not waiting in a retry loop
//  while (true) {
//    if (getConvertLock.try_lock()) {
//      if (activeConvertGets < 36) {
//        activeConvertGets++;
//        getConvertLock.unlock();
//        break;
//      } else {
//        getConvertLock.unlock();
//      }
//    }
//    std::this_thread::sleep_for (std::chrono::milliseconds(retrySleepTimeMS));
//  }
//  Aws::S3::Model::GetObjectRequest getObjectRequest;
  Aws::String bucketName;
//  bucketName = "demo-bucket";
  bucketName = "flexpushdowndb";
//  auto requestKey = "ssb-sf100-sortlineorder/gzip_compression1_csv/lineorder_sharded/lineorder.gz.tbl." + std::to_string(requestNum);
//  auto requestKey = "ssb-sf0.01/csv/date.tbl";
//  auto schema = SSBSchema::date();
//  auto requestKey = "ssb-sf10-sortlineorder/csv/lineorder_sharded/lineorder.tbl." + std::to_string(requestNum);
//  auto requestKey = "minidata.csv";
//  auto requestKey = "ssb-sf100-sortlineorder/csv/lineorder_sharded/lineorder.tbl." + std::to_string(requestNum);
  std::string requestKey = fmt::format("ssb-sf100-sortlineorder/csv_150MB_initial_format/lineorder_sharded/lineorder.tbl.{}", requestNum);
  auto schema = SSBSchema::lineOrder();

  auto requestStartTime = std::chrono::steady_clock::now();
    Aws::S3::Model::GetObjectRequest getObjectRequest;
    getObjectRequest.SetBucket(Aws::String(bucketName));
    getObjectRequest.SetKey(Aws::String(requestKey));

//  SPDLOG_INFO("Starting s3 GetObject request: {} for {}/{}", requestNum, bucketName, requestKey);
  Aws::S3::Model::GetObjectResult getResult;
  uint64_t resultSize;
  while (true) {
    Aws::S3::Model::GetObjectOutcome getObjectOutcome = DefaultS3Client->GetObject(getObjectRequest);
    if (getObjectOutcome.IsSuccess()) {
      SPDLOG_INFO("GET request success: {}", requestNum);
      auto requestStopTime = std::chrono::steady_clock::now();
      auto requestDurationUs = std::chrono::duration_cast<std::chrono::nanoseconds>(
              requestStopTime - requestStartTime).count();
      getResult = getObjectOutcome.GetResultWithOwnership();
      resultSize = getObjectOutcome.GetResult().GetContentLength();
      SPDLOG_DEBUG("GET request finishes: {}, length: {}, took: {}sec, rate: {}MB/s", requestNum, resultSize,
                  (double) (requestDurationUs) / 1.0e9, ((double) resultSize / 1024.0 / 1024.0) / ((double)(requestDurationUs) / 1.0e9));
      break;
//      return requestDurationUs;
    } else {
//      auto requestDurationUs = std::chrono::duration_cast<std::chrono::nanoseconds>(
//            requestStopTime - requestStartTime).count();
      const auto &err = getObjectOutcome.GetError();
      SPDLOG_INFO("Failed to get result of s3 GetObject request: {}, error={}", requestNum, err.GetMessage());
      return 0;
    }
  }
  getBytesReceivedLock.lock();
  receivedBytes += resultSize;
  getBytesReceivedLock.unlock();

  auto convertStartTime = std::chrono::steady_clock::now();
  auto &retrievedFile = getResult.GetBody();

#ifdef __AVX2__
  std::string callerName = "testCaller";
  auto parser = CSVToArrowSIMDStreamParser(callerName, 128 * 1024, retrievedFile, true, schema, schema, false, normal::connector::defaultMiniCatalogue->getCSVFileDelimiter());
  auto tupleSet = parser.constructTupleSet();
  while (true) {
    if (getConvertLock.try_lock()) {
      activeConvertGets--;
      getConvertLock.unlock();
      break;
    }
  }

  // Code snippet below for putting a lock on the parse phase only
//  bool parsingComplete = false;
//  uint64_t retrySleepTimeMS = 1;
//  while (!parsingComplete) {
//    if (getConvertLock.try_lock()) {
//      if (activeConvertGets < 36) {
//        activeConvertGets++;
//        getConvertLock.unlock();
//        auto parser = CSVToArrowSIMDStreamParser(callerName, 128 * 1024, retrievedFile, true, schema, schema, false);
//        auto tupleSet = parser.constructTupleSet();
//        parsingComplete = true;
//        break;
//      } else {
//        getConvertLock.unlock();
//      }
//    }
//  }
  SPDLOG_INFO("Query: {}, Output rows: {}, bytes received: {}", requestNum, tupleSet->numRows(), resultSize);
  auto convertStopTime = std::chrono::steady_clock::now();
  auto convertDurationNs = std::chrono::duration_cast<std::chrono::nanoseconds>(convertStopTime - convertStartTime).count();
//  SPDLOG_DEBUG("Num rows = {}", tupleSet->numRows());
//  SPDLOG_DEBUG("{}", tupleSet->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented, tupleSet->numRows())));
//  SPDLOG_INFO("Total time                         : {}ms", convertDurationNs / 1.0e6);
//  SPDLOG_INFO("Convert result of s3 GetObject request: {}, took: {}sec, {}ns, rate = {}MB/s", requestNum, (double) (convertDurationNs) / 1.0e9,
//              convertDurationNs,((double) resultSize / 1024.0 / 1024.0) / ((double)(convertDurationNs) / 1.0e9));
  return convertDurationNs;
#else
  return 0;
#endif
}

void normal::ssb::concurrentSelectTest(int numRequests) {
  spdlog::set_level(spdlog::level::info);
  std::shared_ptr<Aws::S3::S3Client> client1 = AWSClient::defaultS3Client();
  size_t totalTimeNS = 0;
  size_t totalBytesReturned = 0;
  int numTrials = 3;
  for (int i = 0; i < numTrials; i++) {
    selectReceivedBytes = 0;
//    spdlog::set_level(spdlog::level::off);
    std::vector<std::thread> threadVector = std::vector<std::thread>();
    auto startTime = std::chrono::steady_clock::now();
    for (int j = 0; j < numRequests; j++) {
      threadVector.emplace_back(std::thread([client1, j]() { simpleSelectRequest(client1, j); }));
    }
    for (auto &t: threadVector) {
      t.join();
    }
    auto stopTime = std::chrono::steady_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stopTime - startTime).count();
    totalTimeNS += duration;
    selectBytesReceivedLock.lock();
    size_t returnedBytes = selectReceivedBytes;
    totalBytesReturned += returnedBytes;
    selectBytesReceivedLock.unlock();
    spdlog::set_level(spdlog::level::info);
    SPDLOG_INFO("{} Concurrent Select requests took: {}sec\n Machine transfer rate: {}Gb/s", numRequests,
                (double) (duration) / 1.0e9,
                (((double) returnedBytes * 8 / 1024.0 / 1024.0 / 1024.0) / ((double) (duration) / 1.0e9)));
    std::this_thread::sleep_for (std::chrono::milliseconds(1000));
  }
  double averageTrialTimeNS = ((double) (totalTimeNS) / 1.0e9) / (double) numTrials;
  SPDLOG_INFO("Average for {} took: {}sec\n Average machine transfer rate: {}Gb/s", numRequests,
              averageTrialTimeNS,
              (((double) totalBytesReturned * 8 / numTrials / 1024.0 / 1024.0 / 1024.0) / averageTrialTimeNS));
}

void normal::ssb::concurrentGetTest(int numRequests) {
  spdlog::set_level(spdlog::level::info);
  size_t totalBytesReturned = 0;
  size_t totalTimeNS = 0;
  int numTrials = 1;
  for (int i = 0; i < numTrials; i++) {
    receivedBytes = 0;
    activeConvertGets = 0;
//    spdlog::set_level(spdlog::level::off);
    std::vector<std::thread> threadVector = std::vector<std::thread>();
    auto startTime = std::chrono::steady_clock::now();
    for (int j = 0; j < numRequests; j++) {
      threadVector.emplace_back(std::thread([j]() { simpleGetRequest(j); }));
    }
    for (auto &t: threadVector) {
      t.join();
    }
    auto stopTime = std::chrono::steady_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stopTime - startTime).count();
    totalTimeNS += duration;
    spdlog::set_level(spdlog::level::info);
    getBytesReceivedLock.lock();
    size_t returnedBytes = receivedBytes;
    totalBytesReturned += returnedBytes;
    getBytesReceivedLock.unlock();
    SPDLOG_INFO("{} Concurrent Get requests took: {}sec\n Machine transfer rate: {}Gb/s", numRequests,
                (double) (duration) / 1.0e9,
                (((double) returnedBytes * 8 / 1024.0 / 1024.0 / 1024.0) / ((double) (duration) / 1.0e9)));
    std::this_thread::sleep_for (std::chrono::milliseconds(1000));
  }
  double averageTrialTimeNS = ((double) (totalTimeNS) / 1.0e9) / (double) numTrials;
  SPDLOG_INFO("Average for {} took: {}sec\n Average machine transfer rate: {}Gb/s", numRequests,
              averageTrialTimeNS,
              (((double) totalBytesReturned * 8 / numTrials / 1024.0 / 1024.0 / 1024.0) / averageTrialTimeNS));
}

void normal::ssb::mainTest(size_t cacheSize, int modeType, int cachingPolicyType, const std::string& dirPrefix,
                           size_t networkLimit, bool writeResults) {
  spdlog::set_level(spdlog::level::warn);
  // parameters
  const int warmBatchSize = 50, executeBatchSize = 50;
  std::string bucket_name = "flexpushdowndb";
  normal::connector::defaultMiniCatalogue = normal::connector::MiniCatalogue::defaultMiniCatalogue(bucket_name, dirPrefix);
  normal::cache::beladyMiniCatalogue = normal::connector::MiniCatalogue::defaultMiniCatalogue(bucket_name, dirPrefix);
  if (networkLimit > 0) {
    NetworkLimit = networkLimit;
  }
  DefaultS3Client = AWSClient::defaultS3Client();

  std::shared_ptr<normal::plan::operator_::mode::Mode> mode;
  std::string modeAlias;
  switch (modeType) {
    case 1: mode = normal::plan::operator_::mode::Modes::fullPullupMode(); modeAlias = "fpu"; break;
    case 2: mode = normal::plan::operator_::mode::Modes::fullPushdownMode(); modeAlias = "fpd"; break;
    case 3: mode = normal::plan::operator_::mode::Modes::pullupCachingMode(); modeAlias = "pc"; break;
    case 4: mode = normal::plan::operator_::mode::Modes::hybridCachingMode(); modeAlias = "hc"; break;
    default: throw std::runtime_error("Mode not found, type: " + std::to_string(modeType));
  }

  std::shared_ptr<normal::cache::CachingPolicy> cachingPolicy;
  std::string cachingPolicyAlias;
  switch (cachingPolicyType) {
    case 1: cachingPolicy = LRUCachingPolicy::make(cacheSize, mode); cachingPolicyAlias = "lru"; break;
    case 2: cachingPolicy = FBRSCachingPolicy::make(cacheSize, mode); cachingPolicyAlias = "lfu"; break;
    case 3: cachingPolicy = WFBRCachingPolicy::make(cacheSize, mode); cachingPolicyAlias = "wlfu"; break;
    case 4: cachingPolicy = BeladyCachingPolicy::make(cacheSize, mode); cachingPolicyAlias = "bldy"; break;
    default: throw std::runtime_error("CachingPolicy not found, type: " + std::to_string(cachingPolicyType));
  }

  auto currentPath = std::filesystem::current_path();
  auto sql_file_dir_path = currentPath.append("sql/generated");

  if (cachingPolicy->id() == BELADY) {
    auto beladyCachingPolicy = std::static_pointer_cast<normal::cache::BeladyCachingPolicy>(cachingPolicy);
    generateBeladySegmentKeyAndSqlQueryMappings(mode, beladyCachingPolicy, bucket_name, dirPrefix, warmBatchSize + executeBatchSize, sql_file_dir_path);
    // Generate caching decisions for belady
    SPDLOG_INFO("Generating belady caching decisions. . .");
    beladyCachingPolicy->generateCacheDecisions(warmBatchSize + executeBatchSize);
    SPDLOG_INFO("belady caching decisions generated");
    SPDLOG_DEBUG("Belady caching decisions:\n" + beladyCachingPolicy->printLayoutAfterEveryQuery());
  }

  // interpreter
  normal::sql::Interpreter i(mode, cachingPolicy);
  configureS3ConnectorMultiPartition(i, bucket_name, dirPrefix);

  // execute
  // FIXME: has to make a new one other wise with Airmettle sometimes a req has no end event, unsure why
  DefaultS3Client = AWSClient::defaultS3Client();
  i.boot();
  SPDLOG_CRITICAL("{} mode start", mode->toString());
  if (mode->id() != normal::plan::operator_::mode::ModeId::FullPullup &&
      mode->id() != normal::plan::operator_::mode::ModeId::FullPushdown) {
    SPDLOG_CRITICAL("Cache warm phase:");
    for (auto index = 1; index <= warmBatchSize; ++index) {
      SPDLOG_CRITICAL("sql {}", index);
      if (cachingPolicy->id() == BELADY) {
        normal::cache::beladyMiniCatalogue->setCurrentQueryNum(index);
      }
      auto sql_file_path = sql_file_dir_path.append(fmt::format("{}.sql", index));
      auto sql = read_file(sql_file_path.string());
      executeSql(i, sql, true, writeResults, fmt::format("{}output.txt", index));
      sql_file_dir_path = sql_file_dir_path.parent_path();
    }
    SPDLOG_CRITICAL("Cache warm phase finished");
  } else {
    // execute one query to avoid first-run latency
    SPDLOG_CRITICAL("First-run query:");
    auto sql_file_path = sql_file_dir_path.append(fmt::format("{}.sql", 1));
    auto sql = read_file(sql_file_path.string());
    executeSql(i, sql, false, false, fmt::format("{}output.txt", index));
    sql_file_dir_path = sql_file_dir_path.parent_path();
  }

  // collect warmup metrics for later output
  std::string warmupMetrics = i.showMetrics();
  std::string warmupCacheMetrics = i.getOperatorManager()->showCacheMetrics();
  i.clearMetrics();

  i.getOperatorManager()->clearCacheMetrics();

  // script to collect resource usage
//  system("./scripts/measure_usage_start.sh ~/.aws/my-key-pair.pem scripts/iplist.txt");
  SPDLOG_CRITICAL("Execution phase:");
  for (auto index = warmBatchSize + 1; index <= warmBatchSize + executeBatchSize; ++index) {
    SPDLOG_CRITICAL("sql {}", index - warmBatchSize);
    if (cachingPolicy->id() == BELADY) {
      normal::cache::beladyMiniCatalogue->setCurrentQueryNum(index);
    }
    auto sql_file_path = sql_file_dir_path.append(fmt::format("{}.sql", index));
    auto sql = read_file(sql_file_path.string());
    executeSql(i, sql, true, writeResults, fmt::format("{}output.txt", index));
    sql_file_dir_path = sql_file_dir_path.parent_path();
  }
  SPDLOG_CRITICAL("Execution phase finished");
//  system("./scripts/measure_usage_stop.sh ~/.aws/my-key-pair.pem scripts/iplist.txt");

  SPDLOG_INFO("{} mode finished in dirPrefix: {}\nExecution metrics:\n{}", mode->toString(), dirPrefix, i.showMetrics());
  SPDLOG_INFO("Cache Metrics:\n{}", i.getOperatorManager()->showCacheMetrics());
  SPDLOG_INFO("Cache hit ratios:\n{}", i.showHitRatios());
    SPDLOG_INFO("OnLoad time: {}", i.getCachingPolicy()->onLoadTime);
    SPDLOG_INFO("OnStore time: {}", i.getCachingPolicy()->onStoreTime);
    SPDLOG_INFO("OnToCache time: {}", i.getCachingPolicy()->onToCacheTime);

  auto metricsFilePath = std::filesystem::current_path().append("metrics-" + modeAlias + "-" + cachingPolicyAlias);
  std::ofstream fout(metricsFilePath.string());
  fout << mode->toString() << " mode finished in dirPrefix:" << dirPrefix << "\n";
  fout << "Warmup metrics:\n" << warmupMetrics << "\n";
  fout << "Warmup Cache metrics:\n" << warmupCacheMetrics << "\n";
  fout << "Execution metrics:\n" << i.showMetrics() << "\n";
  fout << "Execution Cache metrics:\n" << i.getOperatorManager()->showCacheMetrics() << "\n";
  fout << "All Cache hit ratios:\n" << i.showHitRatios() << "\n";
//  fout << "Current cache layout:\n" << i.getCachingPolicy()->showCurrentLayout() << "\n";
  fout.flush();
  fout.close();

  i.getOperatorGraph().reset();
  i.stop();
  SPDLOG_INFO("Memory allocated finally: {}", arrow::default_memory_pool()->bytes_allocated());
}

void normal::ssb::perfBatchRun(int modeType, const std::string& dirPrefix, int cacheLoadQueries,
                               int warmupQueriesPerColSize, int columnSizesToTest, int rowSelectivityValuesToTest) {
  spdlog::set_level(spdlog::level::info);
  size_t cacheSize = 64L * 1024 * 1024 * 1024;   // make the cache large so we can hold all segments for any h
  std::string bucket_name = "flexpushdowndb";
  normal::connector::defaultMiniCatalogue = normal::connector::MiniCatalogue::defaultMiniCatalogue(bucket_name, dirPrefix);

  std::shared_ptr<normal::plan::operator_::mode::Mode> mode;
  std::string modeAlias;
  // Currently there are only plans to run these experiments in mode fpd, pc, and hc, however leaving fpu here in case
  // things change and we want to test fpu
  switch (modeType) {
    case 1: mode = normal::plan::operator_::mode::Modes::fullPullupMode(); modeAlias = "fpu"; break;
    case 2: mode = normal::plan::operator_::mode::Modes::fullPushdownMode(); modeAlias = "fpd"; break;
    case 3: mode = normal::plan::operator_::mode::Modes::pullupCachingMode(); modeAlias = "pc"; break;
    case 4: mode = normal::plan::operator_::mode::Modes::hybridCachingMode(); modeAlias = "hc"; break;
    default: throw std::runtime_error("Mode not found, type: " + std::to_string(modeType));
  }
  auto cachingPolicy = FBRSCachingPolicy::make(cacheSize, mode);

  auto currentPath = std::filesystem::current_path();
  auto sql_file_dir_path = currentPath.append("sql/generated");

  // interpreter
  normal::sql::Interpreter i(mode, cachingPolicy);
  configureS3ConnectorMultiPartition(i, bucket_name, dirPrefix);
  // execute
  i.boot();
  SPDLOG_INFO("{} mode start", mode->toString());
  // Run queries (if there are any) to fill the cache
  SPDLOG_INFO("Cache fill phase:");
  for (auto index = 1; index <= cacheLoadQueries; ++index) {
    SPDLOG_INFO("sql {}", index);
    auto sql_file_path = sql_file_dir_path.append(fmt::format("{}.sql", index));
    auto sql = read_file(sql_file_path.string());
    executeSql(i, sql, false, false, fmt::format("{}output.txt", index));
    sql_file_dir_path = sql_file_dir_path.parent_path();
  }
  SPDLOG_INFO("Cache fill phase finished");
  // clear the warmup metrics and disallow caching, we don't need want it for these experiments
  i.clearMetrics();
  i.clearHitRatios();
  i.getOperatorManager()->clearCacheMetrics();
  i.getOperatorManager()->clearCrtQueryMetrics();
  i.getOperatorManager()->clearCrtQueryShardMetrics();
  normal::cache::allowFetchSegments = false;

  SPDLOG_INFO("Test phase:");
  int queryNumOffset = cacheLoadQueries;
  for (int c = 1; c <= columnSizesToTest; c++) {
    for (int r = 1; r <= rowSelectivityValuesToTest + warmupQueriesPerColSize; r++) {
      int sqlIndex = queryNumOffset + (c - 1) * (rowSelectivityValuesToTest + warmupQueriesPerColSize) + r;
      SPDLOG_INFO("sql {}", sqlIndex);
      auto sql_file_path = sql_file_dir_path.append(fmt::format("{}.sql", sqlIndex));
      auto sql = read_file(sql_file_path.string());
      bool saveMetrics = true;
      if (r <= warmupQueriesPerColSize) {
        SPDLOG_INFO("Not saving results for {}", sqlIndex);
        // Don't record metrics for queries that warmup S3Select/S3Get
        saveMetrics = false;
      } else {
        SPDLOG_INFO("Saving results for {}", sqlIndex);
      }
      executeSql(i, sql, saveMetrics, false, fmt::format("{}output.txt", index));
      sql_file_dir_path = sql_file_dir_path.parent_path();
      // Reset the cache metrics for the next query
      i.getOperatorManager()->clearCrtQueryMetrics();
      i.getOperatorManager()->clearCrtQueryShardMetrics();
    }
  }
  SPDLOG_INFO("Test phase finished");

  SPDLOG_INFO("{} mode finished in dirPrefix: {}\nExecution metrics:\n{}", mode->toString(), dirPrefix, i.showMetrics());
  SPDLOG_INFO("Cache Metrics:\n{}", i.getOperatorManager()->showCacheMetrics());
  SPDLOG_INFO("Cache hit ratios:\n{}", i.showHitRatios());
    SPDLOG_INFO("OnLoad time: {}", i.getCachingPolicy()->onLoadTime);
    SPDLOG_INFO("OnStore time: {}", i.getCachingPolicy()->onStoreTime);
    SPDLOG_INFO("OnToCache time: {}", i.getCachingPolicy()->onToCacheTime);

  auto metricsFilePath = std::filesystem::current_path().append("metrics-perf-test");
  std::ofstream fout(metricsFilePath.string());
  fout << mode->toString() << " mode finished in dirPrefix:" << dirPrefix << "\n";
  fout << "Execution metrics:\n" << i.showMetrics() << "\n";
  fout << "All Cache hit ratios:\n" << i.showHitRatios() << "\n";
  fout.flush();
  fout.close();

  i.getOperatorGraph().reset();
  i.stop();
  SPDLOG_INFO("Memory allocated finally: {}", arrow::default_memory_pool()->bytes_allocated());
}

TEST_SUITE ("MainTests" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("SequentialRun" * doctest::skip(true || SKIP_SUITE)) {
  spdlog::set_level(spdlog::level::info);

  // choose whether to use partitioned lineorder
  bool partitioned = true;

  // choose mode
  auto mode0 = normal::plan::operator_::mode::Modes::fullPullupMode();
  auto mode1 = normal::plan::operator_::mode::Modes::fullPushdownMode();
  auto mode2 = normal::plan::operator_::mode::Modes::pullupCachingMode();
  auto mode3 = normal::plan::operator_::mode::Modes::hybridCachingMode();
  auto mode4 = normal::plan::operator_::mode::Modes::hybridCachingLastMode();

  // hardcoded parameters
  std::vector<std::string> sql_file_names = {
          "query1.1.sql", "query1.2.sql", "query1.3.sql",
          "query2.1.sql", "query2.2.sql", "query2.3.sql",
          "query3.1.sql", "query3.2.sql", "query3.3.sql", "query3.4.sql",
          "query4.1.sql", "query4.2.sql", "query4.3.sql"
  };
  std::vector<int> order1 = {
          0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
  };
  std::vector<int> order2 = {
          0, 7, 12, 4, 11, 1, 3, 10, 8, 2, 9, 5, 6
  };
  auto currentPath = std::filesystem::current_path();
  auto sql_file_dir_path = currentPath.append("sql/filterlineorder");
  std::string bucket_name = "s3filter";
  std::string dir_prefix = "ssb-sf1/";

  const auto& chosenMode = mode3;
  // choose caching policy
  auto lru = LRUCachingPolicy::make(1024*1024*300, chosenMode);
  auto fbr = FBRCachingPolicy::make(1024*1024*300, chosenMode);

  // configure interpreter
  normal::sql::Interpreter i(chosenMode, fbr);
  if (partitioned) {
    configureS3ConnectorMultiPartition(i, bucket_name, dir_prefix);
  } else {
    configureS3ConnectorSinglePartition(i, bucket_name, dir_prefix);
  }

  // execute
  i.boot();
  int cnt = 1;
  for (const auto index: order2) {
    // read sql file
    auto sql_file_name = sql_file_names[index];
    auto sql_file_path = sql_file_dir_path.append(sql_file_name);
    SPDLOG_DEBUG(sql_file_dir_path.string());
    auto sql = read_file(sql_file_path.string());
    SPDLOG_INFO("{}-{}: \n{}", cnt++, sql_file_name, sql);

    // execute sql
    executeSql(i, sql, true);
    sql_file_dir_path = sql_file_dir_path.parent_path();
  }

  SPDLOG_INFO("Sequence all finished");
  SPDLOG_INFO("Overall Metrics:\n{}", i.showMetrics());
  SPDLOG_INFO("Cache Metrics:\n{}", i.getOperatorManager()->showCacheMetrics());

  i.stop();
}

TEST_CASE ("GenerateSqlBatchRun" * doctest::skip(true || SKIP_SUITE)) {
  spdlog::set_level(spdlog::level::info);

  // prepare queries
  SqlGenerator sqlGenerator;
  auto sqls = sqlGenerator.generateSqlBatch(100);

  // prepare interpreter
  bool partitioned = false;
  auto mode = normal::plan::operator_::mode::Modes::fullPushdownMode();
  std::string bucket_name = "s3filter";
  std::string dir_prefix = "ssb-sf0.01/";
  normal::sql::Interpreter i;
  if (partitioned) {
    configureS3ConnectorMultiPartition(i, bucket_name, dir_prefix);
  } else {
    configureS3ConnectorSinglePartition(i, bucket_name, dir_prefix);
  }

  // execute
  i.boot();
  int index = 1;
  for (const auto &sql: sqls) {
    SPDLOG_INFO("sql {}: \n{}", index++, sql);
    executeSql(i, sql, true);
  }
  i.stop();

  SPDLOG_INFO("Batch finished");
}
}
