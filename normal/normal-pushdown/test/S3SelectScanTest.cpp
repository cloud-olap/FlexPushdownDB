//
// Created by matt on 5/3/20.
//

#include <memory>
#include <vector>

#include <doctest/doctest.h>

#include <normal/pushdown/cache/CacheLoad.h>
#include <normal/pushdown/s3/S3SelectScan.h>
#include <normal/pushdown/s3/S3SelectScan2.h>
#include <normal/pushdown/merge/Merge.h>
#include <normal/pushdown/collate/Collate.h>
#include <normal/core/graph/OperatorGraph.h>
#include <normal/pushdown/AWSClient.h>
#include <normal/core/ATTIC/Normal.h>
#include <normal/connector/s3/S3Util.h>
#include <normal/connector/s3/S3SelectPartition.h>

#include "TestUtil.h"

using namespace normal::pushdown;
using namespace normal::pushdown::cache;
using namespace normal::pushdown::merge;
using namespace normal::pushdown::test;
using namespace normal::core;
using namespace normal::core::graph;
using namespace normal::connector::s3;

//void run(const std::string &s3Bucket,
//		 const std::string &s3ObjectPrefix,
//		 const std::string &s3Object,
//		 FileType fileType,
//		 const std::vector<std::string> &columnNames) {

//  normal::pushdown::AWSClient client;
//  client.init();
//
//  auto n = Normal::start();
//
//  auto g = n->createQuery();
//
//  auto s3Objects = std::vector{s3Object};
//  auto partitionMap = S3Util::listObjects(s3Bucket, s3ObjectPrefix, s3Objects, AWSClient::defaultS3Client());
//  SPDLOG_DEBUG("Discovered partitions");
//  for (auto &partition : partitionMap) {
//	SPDLOG_DEBUG("  's3://{}/{}': size: {}", s3Bucket, partition.first, partition.second);
//  }
//
//  std::shared_ptr<Partition> partition = std::make_shared<S3SelectPartition>(s3Bucket, s3Object);
//  auto cacheLoad = CacheLoad::make("cache-load", columnNames, {}, partition, 0, partitionMap[s3Object], true);
//  g->put(cacheLoad);
//  auto merge = Merge::make("merge");
//  g->put(merge);
//  auto s3selectScan = S3SelectScan2::make("s3select-scan",
//										  s3Bucket,
//										  s3Object,
//										  "select {} from S3Object",
//										  0,
//										  partitionMap[s3Object],
//										  fileType,
//										  columnNames,
//										  S3SelectCSVParseOptions(",", "\n"),
//										  AWSClient::defaultS3Client(),
//										  false);
//  g->put(s3selectScan);
//
//  auto collate = std::make_shared<Collate>("collate", g->getId());
//  g->put(collate);
//
//  cacheLoad->setHitOperator(merge);
//  merge->setLeftProducer(cacheLoad);
//
//  cacheLoad->setMissOperatorToCache(s3selectScan);
//  s3selectScan->consume(cacheLoad);
//
//  s3selectScan->produce(merge);
//  merge->setRightProducer(s3selectScan);
//
//  merge->produce(collate);
//  collate->consume(merge);
//
//  TestUtil::writeExecutionPlan(*g);
//
//  g->boot();
//  g->start();
//  g->join();
//
//  auto tuples = TupleSet2::create(collate->tuples());
//
//  SPDLOG_DEBUG("Output:\n{}", tuples->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));
//
//  g.reset();
//
//  n->stop();
//
//  client.shutdown();
//}

#define SKIP_SUITE false

TEST_SUITE ("s3select-scan" * doctest::skip(SKIP_SUITE)) {

//TEST_CASE ("s3select-scan-v1-csv" * doctest::skip(false || SKIP_SUITE)) {
//
//  normal::pushdown::AWSClient client;
//  client.init();
//
//  auto n = Normal::start();
//
//  auto g = n->createQuery();
//
//  auto s3selectScan = S3SelectScan::make("s3selectscan",
//										 "pushdowndb",
//										 "ssb-sf0.01/csv/date.tbl",
//										 "select d_datekey from S3Object",
//										 {"d_datekey"},
//										 0,
//										 std::numeric_limits<long>::max(),
//										 S3SelectCSVParseOptions(",", "\n"),
//										 AWSClient::defaultS3Client());
//  g->put(s3selectScan);
//
//  auto collate = std::make_shared<Collate>("collate", g->getId());
//  g->put(collate);
//
//  s3selectScan->produce(collate);
//  collate->consume(s3selectScan);
//
//  TestUtil::writeExecutionPlan(*g);
//
//  g->boot();
//
//  g->start();
//  g->join();
//
//  auto tuples = TupleSet2::create(collate->tuples());
//
//  SPDLOG_DEBUG("Output:\n{}", tuples->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));
//
//  n->stop();
//
//  client.shutdown();
//}

//TEST_CASE ("s3select-parser" * doctest::skip(false || SKIP_SUITE)) {
//
//  Aws::Vector<unsigned char> data{'1', ',', '2', ',', '3', '\n',
//								  '4', ',', '5', ',', '6', '\n',
//								  '7', ',', '8', ',', '9', '\n'};
//
//  for(size_t payloadSize = 1; payloadSize <= data.size(); ++payloadSize) {
//
//	S3SelectParser parser;
//
//	auto selectionStart = data.begin();
//	auto selectionEnd = selectionStart + std::min<long>(data.end() - selectionStart, payloadSize);
//	while (selectionStart != data.end()) {
//
//	  auto payload = Aws::Vector<unsigned char>(selectionStart, selectionEnd);
//
//	  selectionStart = selectionEnd;
//	  selectionEnd = selectionStart + std::min<long>(data.end() - selectionStart, payloadSize);
//
//	  auto expectedTupleSet = parser.parse(payload);
//
//	  // Check for error
//	  if (!expectedTupleSet.has_value())
//			FAIL (expectedTupleSet.error());
//	  auto maybeTupleSet = expectedTupleSet.value();
//
//	  // Check if a tupleset was parsed
//	  if (maybeTupleSet.has_value()) {
//		auto tuples = TupleSet2::create(maybeTupleSet.value());
//		SPDLOG_DEBUG("Output:\n{}", tuples->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));
//	  }
//	}
//  }
//}

//TEST_CASE ("s3select-scan-v2-csv-large" * doctest::skip(false || SKIP_SUITE)) {
//  run("s3filter", "ssb-sf1", "date.tbl", FileType::CSV, {"d_datekey", "D_DATE", "D_DAYOFWEEK", "D_MONTH", "D_YEAR"});
//}
//
//TEST_CASE ("s3select-scan-v2-csv" * doctest::skip(false || SKIP_SUITE)) {
//  run("pushdowndb", "ssb-sf0.01/csv", "date.tbl", FileType::CSV, {"d_datekey"});
//}
//
//TEST_CASE ("s3select-scan-v2-csv-empty" * doctest::skip(false || SKIP_SUITE)) {
//  run("s3filter", "ssb-sf0.01", "supplier.tbl", FileType::CSV, {});
//}
//
//TEST_CASE ("s3select-scan-v2-parquet" * doctest::skip(false || SKIP_SUITE)) {
//  run("s3filter", "ssb-sf0.01/parquet", "supplier.snappy.parquet", FileType::Parquet, {"s_suppkey", "s_name"});
//}
//
//TEST_CASE ("s3select-scan-v2-parquet-empty" * doctest::skip(false || SKIP_SUITE)) {
//  run("s3filter", "ssb-sf0.01/parquet", "supplier.snappy.parquet", FileType::Parquet, {});
//}

}
