//
// Created by matt on 5/3/20.
//

#include <memory>

#include <doctest/doctest.h>

#include <normal/pushdown/cache/CacheLoad.h>
#include <normal/pushdown/collate/Collate.h>
#include <normal/core/OperatorManager.h>
#include <normal/core/graph/OperatorGraph.h>
#include <normal/pushdown/file/FileScan.h>
#include <normal/pushdown/s3/S3SelectScan.h>
#include <normal/connector/local-fs/LocalFilePartition.h>
#include <normal/pushdown/merge/Merge.h>
#include <normal/connector/s3/S3SelectPartition.h>
#include <normal/pushdown/AWSClient.h>
#include "TestUtil.h"

using namespace normal::pushdown;
using namespace normal::pushdown::test;
using namespace normal::tuple;
using namespace normal::core;
using namespace normal::core::graph;
using namespace normal::pushdown::cache;
using namespace normal::pushdown::merge;

#define SKIP_SUITE true

//void makeQuery(const std::vector<std::string>& columnNames, std::shared_ptr<OperatorGraph> &g) {
//
//  auto testFile = filesystem::absolute("data/cache/test.csv");
//  auto numBytesTestFile = filesystem::file_size(testFile);
//
////  auto g = OperatorGraph::make(mgr);
////
////  std::shared_ptr<Partition> partition = std::make_shared<LocalFilePartition>(testFile);
////
////  auto cacheLoad = CacheLoad::make(fmt::format("/query-{}/cache-load", g->getId()),
////								   columnNames,
////								   partition,
////								   0,
////								   numBytesTestFile);
////
////  auto fileScan = FileScan::make(fmt::format("/query-{}/file-scan", g->getId()),
////								 testFile,
////								 columnNames,
////								 0,
////								 numBytesTestFile,
////								 g->getId());
//
//  auto columnNamess = std::vector<std::string>{"lo_orderkey", "lo_orderdate", "lo_extendedprice", "lo_discount"};
//  numBytesTestFile = 5947638;
//  auto partition = std::make_shared<S3SelectPartition>("s3filter", "ssb-sf0.01/lineorder.tbl", numBytesTestFile);
//  auto cacheLoad = CacheLoad::make(fmt::format("/query-{}/cache-load", g->getId()),
//								   columnNamess,
//                   std::vector<std::string>(),
//								   partition,
//								   0,
//								   numBytesTestFile,
//								   true);
//
//  normal::pushdown::AWSClient client;
//  client.init();
//  auto fileScan = S3SelectScan::make("s3scan",
//                                     partition->getBucket(),
//                                     partition->getObject(),
//                                     "",
//                                     columnNamess,
//                                     0,
//                                     numBytesTestFile,
//                                     S3SelectCSVParseOptions(",", "\n"),
//                                     client.defaultS3Client(),
//                                     false);
//
//  auto merge = Merge::make(fmt::format("/query-{}/merge", g->getId()));
//
//  auto collate = std::make_shared<Collate>(fmt::format("/query-{}/collate", g->getId()), g->getId());
//
//  cacheLoad->setHitOperator(merge);
//  merge->setLeftProducer(cacheLoad);
//
//  cacheLoad->setMissOperatorToCache(fileScan);
//  fileScan->consume(cacheLoad);
//
//  fileScan->produce(merge);
//  merge->setRightProducer(fileScan);
//
//  merge->produce(collate);
//  collate->consume(merge);
//
//  g->put(cacheLoad);
//  g->put(fileScan);
//  g->put(merge);
//  g->put(collate);
//
////  return g;
//}

/**
 * Test running a query multiple times to test cache hits on second run
 */
TEST_SUITE ("cache" * doctest::skip(SKIP_SUITE)) {

//TEST_CASE ("multi-filescan-collate" * doctest::skip(false || SKIP_SUITE)) {
//
//  auto mgr = std::make_shared<OperatorManager>();
//  mgr->boot();
//  mgr->start();
//
//  /**
//   * Run query 1 which should ensure columns a and b are in the cache
//   */
//  auto g1 = OperatorGraph::make(mgr);
//  makeQuery({"a", "b", "c"}, g1);
//  TestUtil::writeExecutionPlan(*g1);
//  auto collate1 = std::static_pointer_cast<Collate>(g1->getOperator(fmt::format("/query-{}/collate", g1->getId())));
//
//  g1->boot();
//  g1->start();
//  g1->join();
//
//  auto tuples1 = collate1->tuples();
//
//  SPDLOG_DEBUG("Output:\n{}", tuples1->toString());
//
////	  CHECK(tuples1->numRows() == 3);
////	  CHECK(tuples1->numColumns() == 3);
//
//  /**
//    * Run query 2 which should use columns a and b from the cache and load column c from the file
//	*/
//  auto g2 = OperatorGraph::make(mgr);
//  makeQuery({"a", "b", "c"}, g2);
//  TestUtil::writeExecutionPlan(*g2);
//  auto collate2 = std::static_pointer_cast<Collate>(g2->getOperator(fmt::format("/query-{}/collate", g2->getId())));
//
//  g2->boot();
//  g2->start();
//  g2->join();
//
//  auto tuples2 = collate2->tuples();
//
//  SPDLOG_DEBUG("Output:\n{}", tuples2->toString());
//
////	  CHECK(tuples2->numRows() == 3);
////	  CHECK(tuples2->numColumns() == 3);
//
////  /**
////    * Run query 3 which should use columns a and b from the cache and load column c from the file
////	*/
////  auto g3 = makeQuery({"a", "b", "c"}, mgr);
////  TestUtil::writeExecutionPlan(*g3);
////  auto collate3 = std::static_pointer_cast<Collate>(g3->getOperator(fmt::format("/query-{}/collate", g3->getId())));
////
////  g3->boot();
////  g3->start();
////  g3->join();
////
////  auto tuples3 = collate3->tuples();
////
////  SPDLOG_DEBUG("Output:\n{}", tuples3->toString());
////
//////	  CHECK(tuples2->numRows() == 3);
//////	  CHECK(tuples2->numColumns() == 3);
//
//  mgr->stop();
//}
//
}
