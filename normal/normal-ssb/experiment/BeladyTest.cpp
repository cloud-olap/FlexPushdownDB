//
// Created by Matt Woicik on 9/16/20.
//

#include <doctest/doctest.h>
#include <normal/sql/Interpreter.h>
#include <normal/ssb/TestUtil.h>
#include <normal/pushdown/Collate.h>
#include <normal/connector/s3/S3SelectExplicitPartitioningScheme.h>
#include <normal/pushdown/Util.h>
#include <normal/plan/mode/Modes.h>
#include <normal/connector/s3/S3Util.h>
#include <normal/cache/LRUCachingPolicy.h>
#include <normal/cache/FBRCachingPolicy.h>
#include <normal/cache/BeladyCachingPolicy.h>
#include "ExperimentUtil.h"
#include <normal/ssb/SqlGenerator.h>
#include <normal/plan/Globals.h>
#include <normal/cache/Globals.h>
#include <normal/connector/MiniCatalogue.h>
#include <normal/pushdown/s3/S3SelectScan.h>
#include <iostream>
#include <fstream>

#define SKIP_SUITE true

using namespace normal::ssb;

// not the best solution, should make a header file down the road most likely but this will work for now
void configureS3ConnectorMultiPartition(normal::sql::Interpreter &i, std::string bucket_name, std::string dir_prefix);
std::shared_ptr<TupleSet2> executeSql(normal::sql::Interpreter &i, const std::string &sql, bool saveMetrics);

size_t getColumnSizeInBytes(std::string s3Bucket, std::string s3Object, std::string tableName, std::string queryColumn, long numBytes) {
  // operators
  std::vector<std::string> s3Objects = {s3Object};
  auto scanRanges = normal::pushdown::Util::ranges<long>(0, numBytes, 1);
  std::vector<std::string> columns = {queryColumn};
  SPDLOG_INFO("Starting S3SelectScan for: {} column {}", s3Object, queryColumn);
  auto s3Scan = S3SelectScan::make(
          "s3select - " + s3Object + "-" + queryColumn,
          s3Bucket,
          s3Object,
          "",
          columns,     // actually useless, will use columnNames from ScanMessage
          scanRanges[0].first,
          scanRanges[0].second,
          normal::connector::defaultMiniCatalogue->getSchema(tableName),
          S3SelectCSVParseOptions(",", "\n"),
          normal::plan::DefaultS3Client,
          true,
          true,
          0);

  auto collate = std::make_shared<normal::pushdown::Collate>("collate", 0);

  // wire up
  auto mgr = std::make_shared<OperatorManager>();
  auto opGraph = std::make_shared<OperatorGraph>(0, mgr);
  s3Scan->produce(collate);
  collate->consume(s3Scan);
  opGraph->put(s3Scan);
  opGraph->put(collate);

  // execute
  opGraph->boot();
  opGraph->start();
  opGraph->join();
  auto tuples = std::static_pointer_cast<normal::pushdown::Collate>(opGraph->getOperator("collate"))->tuples();
//  mgr->stop();
  opGraph->close();

  auto tupleSet = TupleSet2::create(tuples);

  auto potentialColumn = tupleSet->getColumnByName(queryColumn);
  if (!potentialColumn.has_value()) {
    SPDLOG_INFO("Failed to get the column for column {} in s3Object {}", queryColumn, s3Object);
  }

  auto column = potentialColumn.value();
  size_t columnSize = column->size();
  return columnSize;
  return 0;
}

struct segment_info_t {
    std::string objectName;
    std::string column;
    unsigned long startOffset;
    unsigned long endOffset;
    size_t sizeInBytes;
};

// currently generates Belady segment info metadata and outputs it to the console.
// This info has already been added in the corresponding metadata files so it is not written to one.
// This requires the other corresponding metadata files to be set up
// so that at least the partition numbers and columns for each schema are known
void generateBeladyMetadata(std::string s3Bucket, std::string dir_prefix) {
  // store this mapping so we can get the corresponding s3Object path without the schema later
  auto s3ObjectToS3ObjectMinusSchemaMap = std::make_shared<std::unordered_map<std::string, std::string>>();

  // get partitionNums
  auto s3ObjectsMap = std::make_shared<std::unordered_map<std::string, std::shared_ptr<std::vector<std::string>>>>();
  auto partitionNums = normal::cache::beladyMiniCatalogue->partitionNums();

  for (auto const &partitionNumEntry: *partitionNums) {
    auto tableName = partitionNumEntry.first;
    auto partitionNum = partitionNumEntry.second;
    auto objects = std::make_shared<std::vector<std::string>>();

    if (partitionNum == 1) {
      std::string s3Object = dir_prefix + tableName + ".tbl";
      std::string s3ObjectMinusSchema = tableName + ".tbl";
      objects->emplace_back(s3Object);
      s3ObjectsMap->emplace(tableName, objects);
      s3ObjectToS3ObjectMinusSchemaMap->emplace(s3Object, s3ObjectMinusSchema);
    } else {
      for (int i = 0; i < partitionNum; i++) {
        std::string s3Object = fmt::format("{0}{1}_sharded/{1}.tbl.{2}", dir_prefix, tableName, i);
        std::string s3ObjectMinusSchema = fmt::format("{0}_sharded/{0}.tbl.{1}", tableName, i);
        objects->emplace_back(s3Object);
        s3ObjectToS3ObjectMinusSchemaMap->emplace(s3Object, s3ObjectMinusSchema);
      }
      s3ObjectsMap->emplace(tableName, objects);
    }
  }

  // Create lookup table for the numBytes, this value is also used as the endOffset currently
  auto s3Objects = std::make_shared<std::vector<std::string>>();
  for (auto const &s3ObjectPair: *s3ObjectsMap) {
    auto objects = s3ObjectPair.second;
    s3Objects->insert(s3Objects->end(), objects->begin(), objects->end());
  }
  auto objectNumBytes_Map = normal::connector::s3::S3Util::listObjects(s3Bucket, dir_prefix, *s3Objects, normal::plan::DefaultS3Client);

  // Populate all segment data
  auto segmentInfoMetadata = std::make_shared<std::vector<std::shared_ptr<segment_info_t>>>();

  for (auto const &s3ObjectPair: *s3ObjectsMap) {
    auto tableName = s3ObjectPair.first;
    auto objects = s3ObjectPair.second;
    auto partitioningScheme = std::make_shared<S3SelectExplicitPartitioningScheme>();
    for (auto const &s3Object: *objects) {
      auto numBytes = objectNumBytes_Map.find(s3Object)->second;

      auto columns = normal::cache::beladyMiniCatalogue->getColumnsOfTable(tableName);
      for (std::string column: *columns) {
        auto segmentInfo = std::make_shared<segment_info_t>();
        auto s3ObjectToNameMinusSchema = s3ObjectToS3ObjectMinusSchemaMap->find(s3Object);
        if (s3ObjectToNameMinusSchema == s3ObjectToS3ObjectMinusSchemaMap->end()) {
          throw std::runtime_error("Error, " + s3Object + " name without schema not found");
        }
        segmentInfo->objectName = s3ObjectToNameMinusSchema->second;
        segmentInfo->column = column;
        segmentInfo->startOffset = 0;
        segmentInfo->endOffset = numBytes;
        segmentInfo->sizeInBytes = getColumnSizeInBytes(s3Bucket, s3Object, tableName, column, numBytes);
        segmentInfoMetadata->push_back(segmentInfo);
      }
    }
  }
  SPDLOG_INFO("Writing segment info for {} to output.txt", dir_prefix);
  std::ofstream outputFile;
  outputFile.open ("output.txt");
  for (auto segmentInfo : *segmentInfoMetadata) {
//    std::cout << segmentInfo->objectName << ","
//              << segmentInfo->column << ","
//              << segmentInfo->startOffset << ","
//              << segmentInfo->endOffset << ","
//              << segmentInfo->sizeInBytes << "\n";
      outputFile << segmentInfo->objectName << ","
              << segmentInfo->column << ","
              << segmentInfo->startOffset << ","
              << segmentInfo->endOffset << ","
              << segmentInfo->sizeInBytes << "\n";
  }
  outputFile.close();
//  std::cout.flush();
}

void generateSegmentKeyAndSqlQueryMappings(std::shared_ptr<normal::plan::operator_::mode::Mode> mode, std::shared_ptr<normal::cache::BeladyCachingPolicy> beladyCachingPolicy,
                                           std::string bucket_name, std::string dir_prefix, int numQueries, filesystem::path sql_file_dir_path) {
  normal::sql::Interpreter i(mode, beladyCachingPolicy);
  configureS3ConnectorMultiPartition(i, bucket_name, dir_prefix);
  i.boot();
  // populate mapping of SegmentKey->[Query #s Segment is used in] and
  // QueryNumber->[Involved Segment Keys] and set these in the beladyMiniCatalogue
  for (auto queryNum = 1; queryNum <= numQueries; ++queryNum) {
    SPDLOG_INFO("processing segments in query {}", queryNum);
    auto sql_file_path = sql_file_dir_path.append(fmt::format("{}.sql", queryNum));
    auto sql = ExperimentUtil::read_file(sql_file_path.string());
    // get the related segments from the query:
    i.clearOperatorGraph();
    i.parse(sql);

    auto logicalPlan = i.getLogicalPlan();
    auto logicalOperators = logicalPlan->getOperators();
    for (auto logicalOp: *logicalOperators) {
      auto involvedSegmentKeys = logicalOp->extractSegmentKeys();
      for (auto segmentKey: *involvedSegmentKeys) {
        normal::cache::beladyMiniCatalogue->addToSegmentQueryNumMappings(queryNum, segmentKey);
      }
    }
    i.getOperatorGraph().reset();
    sql_file_dir_path = sql_file_dir_path.parent_path();
  }
}

TEST_SUITE ("BeladyTests" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("BeladyGenerateMetadata" * doctest::skip(true || SKIP_SUITE)) {
  spdlog::set_level(spdlog::level::info);
  std::string bucket_name = "pushdowndb";
  std::string dir_prefix = "ssb-sf10-sortlineorder/csv/";
  normal::cache::beladyMiniCatalogue = normal::connector::MiniCatalogue::defaultMiniCatalogue(bucket_name, dir_prefix);
  generateBeladyMetadata(bucket_name, dir_prefix);
}

TEST_CASE ("BeladyExperiment" * doctest::skip(true || SKIP_SUITE)) {
  spdlog::set_level(spdlog::level::info);
  std::string bucket_name = "pushdowndb";
  std::string dir_prefix = "ssb-sf10-sortlineorder/csv/";
  normal::cache::beladyMiniCatalogue = normal::connector::MiniCatalogue::defaultMiniCatalogue(bucket_name, dir_prefix);

  // Use these values when running experiments
  const int warmBatchSize = 50, executeBatchSize = 50;
  const size_t cacheSize = 1024*1024*1024;
  // Temporary values to use when running smaller experiments for small changes
//  const int warmBatchSize = 2, executeBatchSize = 10;
//  const size_t cacheSize = 30*1024*1024;

  // Only run this if you want to generate new Belady metadata.
  // you will also have to modify the code to redirect this output to a file as it is currently set up
  // to just go to the default std::cout
  // generateBeladyMetadata(bucket_name, dir_prefix);

  auto mode = normal::plan::operator_::mode::Modes::hybridCachingMode();
  auto lru = LRUCachingPolicy::make(cacheSize, mode);
  auto fbr = FBRCachingPolicy::make(cacheSize, mode);
  auto belady = BeladyCachingPolicy::make(cacheSize, mode);

  auto currentPath = filesystem::current_path();
  auto sql_file_dir_path = currentPath.append("sql/generated");

  // interpreter
  normal::sql::Interpreter i(mode, belady);
  configureS3ConnectorMultiPartition(i, bucket_name, dir_prefix);
  i.boot();

  // populate mapping of SegmentKey->[Query #s Segment is used in] and
  // QueryNumber->[Involved Segment Keys] and set these in the beladyMiniCatalogue
//  generateSegmentKeyAndSqlQueryMappings(i, warmBatchSize + executeBatchSize, sql_file_dir_path);
  belady->generateCacheDecisions(warmBatchSize + executeBatchSize);

  for (auto index = 1; index <= warmBatchSize; ++index) {
    normal::cache::beladyMiniCatalogue->setCurrentQueryNum(index);
    SPDLOG_INFO("sql {}", index);
    auto sql_file_path = sql_file_dir_path.append(fmt::format("{}.sql", index));
    auto sql = ExperimentUtil::read_file(sql_file_path.string());
    executeSql(i, sql, false);
    sql_file_dir_path = sql_file_dir_path.parent_path();
  }
  SPDLOG_INFO("Cache warm phase finished");

  i.getOperatorManager()->clearCacheMetrics();

  SPDLOG_INFO("Execution phase:");
  for (auto index = warmBatchSize + 1; index <= warmBatchSize + executeBatchSize; ++index) {
    normal::cache::beladyMiniCatalogue->setCurrentQueryNum(index);
    SPDLOG_INFO("sql {}", index - warmBatchSize);
    auto sql_file_path = sql_file_dir_path.append(fmt::format("{}.sql", index));
    auto sql = ExperimentUtil::read_file(sql_file_path.string());
    executeSql(i, sql, true);
    sql_file_dir_path = sql_file_dir_path.parent_path();
  }
  SPDLOG_INFO("Execution phase finished");

  SPDLOG_INFO("{} mode finished\nOverall metrics:\n{}", mode->toString(), i.showMetrics());
  SPDLOG_INFO("Cache Metrics:\n{}", i.getOperatorManager()->showCacheMetrics());

  auto metricsFilePath = filesystem::current_path().append("metrics");
  std::ofstream fout(metricsFilePath.string());
  fout << mode->toString() << " mode finished\nOverall metrics:\n" << i.showMetrics() << "\n";
  fout << "Cache metrics:\n" << i.getOperatorManager()->showCacheMetrics() << "\n";
  fout.flush();
  fout.close();

  i.getOperatorGraph().reset();
  i.stop();
  SPDLOG_INFO("Memory allocated finally: {}", arrow::default_memory_pool()->bytes_allocated());
}

}
