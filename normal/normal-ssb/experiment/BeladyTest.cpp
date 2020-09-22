//
// Created by Matt Woicik on 9/16/20.
//

#include <doctest/doctest.h>
#include <normal/sql/Interpreter.h>
#include <normal/ssb/TestUtil.h>
#include <normal/pushdown/Collate.h>
#include <normal/connector/s3/S3SelectConnector.h>
#include <normal/connector/s3/S3SelectExplicitPartitioningScheme.h>
#include <normal/connector/s3/S3SelectCatalogueEntry.h>
#include <normal/pushdown/Util.h>
#include <normal/plan/mode/Modes.h>
#include <normal/connector/s3/S3Util.h>
#include <normal/cache/LRUCachingPolicy.h>
#include <normal/cache/FBRCachingPolicy.h>
#include "ExperimentUtil.h"
#include <normal/ssb/SqlGenerator.h>
#include <normal/plan/Globals.h>
#include <normal/cache/Globals.h>
#include <normal/connector/MiniCatalogue.h>
#include <normal/pushdown/s3/S3SelectScan.h>

#define SKIP_SUITE false

using namespace normal::ssb;

size_t getColumnSizeInBytes(std::string s3Bucket, std::string s3Object, std::string queryColumn, long numBytes) {
  // operators
  std::vector<std::string> s3Objects = {s3Object};
  auto scanRanges = normal::pushdown::Util::ranges<long>(0, numBytes, 1);
  std::vector<std::string> columns = {queryColumn};
  auto lineorderScan = normal::pushdown::S3SelectScan::make(
          "SimpleScan-" + s3Object + ":" + queryColumn,
          s3Bucket,
          s3Object,
          "",
          columns,
          scanRanges[0].first,
          scanRanges[0].second,
          normal::pushdown::S3SelectCSVParseOptions(",", "\n"),
          normal::plan::DefaultS3Client,
          true);

  auto collate = std::make_shared<normal::pushdown::Collate>("collate", 0);

  // wire up
  auto mgr = std::make_shared<OperatorManager>();
  lineorderScan->produce(collate);
  collate->consume(lineorderScan);
  mgr->put(lineorderScan);
  mgr->put(collate);

  // execute
  mgr->boot();
  mgr->start();
  mgr->join();
  auto tuples = std::static_pointer_cast<normal::pushdown::Collate>(mgr->getOperator("collate"))->tuples();
  mgr->stop();

  auto tupleSet = TupleSet2::create(tuples);

  auto potentialColumn = tupleSet->getColumnByName(queryColumn);
  if (!potentialColumn.has_value()) {
    SPDLOG_INFO("Failed to get the column for column {} in s3Object {}", queryColumn, s3Object);
  }

  auto column = potentialColumn.value();
  size_t columnSize = column->size();
  return columnSize;
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
  auto partitionNums = normal::connector::defaultMiniCatalogue->partitionNums();

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

      auto columns = normal::connector::defaultMiniCatalogue->getColumnsOfTable(tableName);
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
        segmentInfo->sizeInBytes = getColumnSizeInBytes(s3Bucket, s3Object, column, numBytes);
        segmentInfoMetadata->push_back(segmentInfo);
      }
    }
  }
  std::cout << "\n\nPRINTING OUT SEGMENT INFO\n\n";
  for (auto segmentInfo : *segmentInfoMetadata) {
    std::cout << segmentInfo->objectName << ","
              << segmentInfo->column << ","
              << segmentInfo->startOffset << ","
              << segmentInfo->endOffset << ","
              << segmentInfo->sizeInBytes << "\n";
  }
}

void configureS3ConnectorMultiPartition2(normal::sql::Interpreter &i, std::string bucket_name, std::string dir_prefix) {
  auto conn = std::make_shared<normal::connector::s3::S3SelectConnector>("s3_select");
  auto cat = std::make_shared<normal::connector::Catalogue>("s3_select", conn);

  // get partitionNums
  auto s3ObjectsMap = std::make_shared<std::unordered_map<std::string, std::shared_ptr<std::vector<std::string>>>>();
  auto partitionNums = normal::connector::defaultMiniCatalogue->partitionNums();
  for (auto const &partitionNumEntry: *partitionNums) {
    auto tableName = partitionNumEntry.first;
    auto partitionNum = partitionNumEntry.second;
    auto objects = std::make_shared<std::vector<std::string>>();
    if (partitionNum == 1) {
      objects->emplace_back(dir_prefix + tableName + ".tbl");
      s3ObjectsMap->emplace(tableName, objects);
    } else {
      for (int i = 0; i < partitionNum; i++) {
        objects->emplace_back(fmt::format("{0}{1}_sharded/{1}.tbl.{2}", dir_prefix, tableName, i));
      }
      s3ObjectsMap->emplace(tableName, objects);
    }
  }

  // look up tables
  auto s3Objects = std::make_shared<std::vector<std::string>>();
  for (auto const &s3ObjectPair: *s3ObjectsMap) {
    auto objects = s3ObjectPair.second;
    s3Objects->insert(s3Objects->end(), objects->begin(), objects->end());
  }
  auto objectNumBytes_Map = normal::connector::s3::S3Util::listObjects(bucket_name, dir_prefix, *s3Objects, normal::plan::DefaultS3Client);

  // configure s3Connector
  for (auto const &s3ObjectPair: *s3ObjectsMap) {
    auto tableName = s3ObjectPair.first;
    auto objects = s3ObjectPair.second;
    auto partitioningScheme = std::make_shared<S3SelectExplicitPartitioningScheme>();
    for (auto const &s3Object: *objects) {
      auto numBytes = objectNumBytes_Map.find(s3Object)->second;
      partitioningScheme->add(std::make_shared<S3SelectPartition>(bucket_name, s3Object, numBytes));
    }
    cat->put(std::make_shared<normal::connector::s3::S3SelectCatalogueEntry>(tableName, partitioningScheme, cat));
  }
  i.put(cat);
}

std::shared_ptr<std::unordered_map<std::shared_ptr<SegmentKey>, std::shared_ptr<std::set<int>>, SegmentKeyPointerHash, SegmentKeyPointerPredicate>>
generateSegmentKeyToSqlQueryMapping(normal::sql::Interpreter &i, int numQueries, filesystem::path sql_file_dir_path) {
  // populate mapping of SegmentKey->[Query #s Segment is used in]
  auto segmentKeysToInvolvedQueryNums = std::make_shared<std::unordered_map<std::shared_ptr<SegmentKey>, std::shared_ptr<std::set<int>>, SegmentKeyPointerHash, SegmentKeyPointerPredicate>>();

  for (auto queryNum = 1; queryNum <= numQueries; ++queryNum) {
    SPDLOG_INFO("processing segments in query {}", queryNum);
    auto sql_file_path = sql_file_dir_path.append(fmt::format("{}.sql", queryNum));
    auto sql = ExperimentUtil::read_file(sql_file_path.string());
    // get the related segments from the query:
    i.getOperatorManager()->getSegmentCacheActor()->ctx()->operatorMap().clearForSegmentCache();
    i.clearOperatorGraph();

    i.parse(sql);

    auto logicalPlan = i.getLogicalPlan();
    auto logicalOperators = logicalPlan->getOperators();
    for (auto logicalOp: *logicalOperators) {
      auto involvedSegmentKeys = logicalOp->extractSegmentKeys();
      for (auto segmentKey: *involvedSegmentKeys) {
        auto keyEntry = segmentKeysToInvolvedQueryNums->find(segmentKey);
        if (keyEntry != segmentKeysToInvolvedQueryNums->end()) {
          keyEntry->second->insert(queryNum);
        } else {
          // first time seeing this key, create a set entry for it
          auto queryNumbers = std::make_shared<std::set<int>>();
          queryNumbers->insert(queryNum);
          segmentKeysToInvolvedQueryNums->emplace(segmentKey, queryNumbers);
        }
      }
    }
    i.getOperatorGraph().reset();
    sql_file_dir_path = sql_file_dir_path.parent_path();
  }
  return segmentKeysToInvolvedQueryNums;
}

TEST_SUITE ("BeladyTests" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("GenerateBeladyMetadataExperiment" * doctest::skip(false || SKIP_SUITE)) {
  spdlog::set_level(spdlog::level::info);
  std::string bucket_name = "pushdowndb";
  std::string dir_prefix = "ssb-sf1-sortlineorder/csv/";

  // Only run this if you want to generate new Belady metadata.
//  generateBeladyMetadata(bucket_name, dir_prefix);

  const int warmBatchSize = 0, executeBatchSize = 100;
  const size_t cacheSize = 1024*1024*1024;

  auto mode = normal::plan::operator_::mode::Modes::hybridCachingMode();
  // TODO: modify this to use BeladyCachingPolicy once implemented
  auto lru = LRUCachingPolicy::make(cacheSize, mode);
  auto fbr = FBRCachingPolicy::make(cacheSize, mode);

  auto currentPath = filesystem::current_path();
  auto sql_file_dir_path = currentPath.append("sql/generated");

  // interpreter
  normal::sql::Interpreter i(mode, fbr);
  configureS3ConnectorMultiPartition2(i, bucket_name, dir_prefix);
  i.boot();

  // get mapping of SegmentKey->[Query #s Segment is used in] and set this in the defaultMiniCatalogue
  auto segmentKeysToInvolvedQueryNums = generateSegmentKeyToSqlQueryMapping(i, warmBatchSize + executeBatchSize, sql_file_dir_path);
  normal::connector::defaultMiniCatalogue->setSegmentKeysToInvolvedQueryNums(segmentKeysToInvolvedQueryNums);



//  std::cout << "\n\nPRINT MAPPING OF segmentKey to Query Numbers\n\n";
//
//  for (auto segmentKeyToQueries: *segmentKeysToInvolvedQueryNums) {
//    std::cout << segmentKeyToQueries.first->toString() << " -> ";
//    for (int queryNum: *segmentKeyToQueries.second) {
//      std::cout << queryNum << ", ";
//    }
//    std::cout << "\n";
//  }

}

}
