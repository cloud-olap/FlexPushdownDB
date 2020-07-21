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

/**
 * Test running a query multiple times to test cache hits on second run
 */
TEST_SUITE ("cache" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("multi-filescan-collate" * doctest::skip(false || SKIP_SUITE)) {

  auto testFile = filesystem::absolute("data/cache/test.csv");
  auto numBytesTestFile = filesystem::file_size(testFile);

  auto mgr = std::make_shared<OperatorManager>();
  mgr->boot();
  mgr->start();

  auto g = OperatorGraph::make(mgr);

  std::shared_ptr<Partition> partition = std::make_shared<LocalFilePartition>(testFile);

  std::vector<std::string> columnNames{"a", "b", "c"};

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

  TestUtil::writeExecutionPlan(*g);

  g->boot();
  g->start();
  g->join();

  auto tuples = collate->tuples();

  SPDLOG_DEBUG("Output:\n{}", tuples->toString());

	  CHECK(tuples->numRows() == 3);
	  CHECK(tuples->numColumns() == 3);

  mgr->stop();
}

}
