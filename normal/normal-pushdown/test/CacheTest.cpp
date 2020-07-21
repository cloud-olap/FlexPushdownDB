//
// Created by matt on 5/3/20.
//

#include <memory>

#include <doctest/doctest.h>

#include <normal/pushdown/cache/CacheLoad.h>
#include <normal/pushdown/Collate.h>
#include <normal/core/OperatorManager.h>
#include <normal/core/graph/OperatorGraph.h>
#include <normal/pushdown/file/FileScan.h>
#include <normal/connector/local-fs/LocalFilePartition.h>
#include <normal/pushdown/merge/MergeOperator.h>
#include "TestUtil.h"

using namespace normal::pushdown;
using namespace normal::pushdown::test;
using namespace normal::tuple;
using namespace normal::core;
using namespace normal::core::graph;
using namespace normal::pushdown::cache;
using namespace normal::pushdown::merge;

#define SKIP_SUITE false

std::shared_ptr<OperatorGraph> makeQuery(const std::vector<std::string>& columnNames, std::shared_ptr<OperatorManager> &mgr) {

  auto testFile = filesystem::absolute("data/cache/test.csv");
  auto numBytesTestFile = filesystem::file_size(testFile);

  auto g = OperatorGraph::make(mgr);

  std::shared_ptr<Partition> partition = std::make_shared<LocalFilePartition>(testFile);

  auto cacheLoad = CacheLoad::make(fmt::format("/query-{}/cache-load", g->getId()),
								   columnNames,
								   partition,
								   0,
								   numBytesTestFile);

  auto fileScan = FileScan::make(fmt::format("/query-{}/file-scan", g->getId()),
								 testFile,
								 columnNames,
								 0,
								 numBytesTestFile,
								 g->getId());

  auto merge = MergeOperator::make(fmt::format("/query-{}/merge", g->getId()));

  auto collate = std::make_shared<Collate>(fmt::format("/query-{}/collate", g->getId()), g->getId());

  cacheLoad->setHitOperator(merge);
  merge->consume(cacheLoad);

  cacheLoad->setMissOperator(fileScan);
  fileScan->consume(cacheLoad);

  fileScan->produce(merge);
  merge->consume(fileScan);

  merge->produce(collate);
  collate->consume(merge);

  g->put(cacheLoad);
  g->put(fileScan);
  g->put(merge);
  g->put(collate);

  return g;
}

/**
 * Test running a query multiple times to test cache hits on second run
 */
TEST_SUITE ("cache" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("multi-filescan-collate" * doctest::skip(false || SKIP_SUITE)) {

  auto mgr = std::make_shared<OperatorManager>();
  mgr->boot();
  mgr->start();

  /**
   * Run query 1 which should ensure columns a and b are in the cache
   */
  auto g1 = makeQuery({"a", "b"}, mgr);
  TestUtil::writeExecutionPlan(*g1);
  auto collate1 = std::static_pointer_cast<Collate>(g1->getOperator(fmt::format("/query-{}/collate", g1->getId())));

  g1->boot();
  g1->start();
  g1->join();

  auto tuples1 = collate1->tuples();

  SPDLOG_DEBUG("Output:\n{}", tuples1->toString());

	  CHECK(tuples1->numRows() == 3);
	  CHECK(tuples1->numColumns() == 2);

  /**
    * Run query 2 which should use columns a and b from the cache and load column c from the file
	*/
  auto g2 = makeQuery({"a", "b", "c"}, mgr);
  TestUtil::writeExecutionPlan(*g2);
  auto collate2 = std::static_pointer_cast<Collate>(g2->getOperator(fmt::format("/query-{}/collate", g2->getId())));

  g2->boot();
  g2->start();
  g2->join();

  auto tuples2 = collate2->tuples();

  SPDLOG_DEBUG("Output:\n{}", tuples2->toString());

	  CHECK(tuples2->numRows() == 3);
	  CHECK(tuples2->numColumns() == 3);

  mgr->stop();
}

}
