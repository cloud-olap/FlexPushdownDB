//
// Created by matt on 5/3/20.
//

#include <memory>

#include <doctest/doctest.h>

#include <normal/pushdown/Collate.h>
#include <normal/core/OperatorManager.h>
#include <normal/core/graph/OperatorGraph.h>
#include <normal/pushdown/file/FileScan.h>
#include "TestUtil.h"

using namespace normal::pushdown;
using namespace normal::pushdown::test;
using namespace normal::tuple;
using namespace normal::core;
using namespace normal::core::graph;

#define SKIP_SUITE true

/**
 * Test running a query multiple times to test cache hits on second run
 */
TEST_SUITE ("cache" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("multi-filescan-collate" * doctest::skip(false || SKIP_SUITE)) {

  auto mgr = std::make_shared<OperatorManager>();

  auto g = OperatorGraph::make(mgr);

  auto fileScan = FileScan::make("fileScan",
								 "data/cache/test.csv",
								 {"a", "b", "c"},
								 0,
								 1023,
								 g->getId());
  auto collate = std::make_shared<Collate>("collate", g->getId());

  fileScan->produce(collate);
  collate->consume(fileScan);

  mgr->put(fileScan);
  mgr->put(collate);

  TestUtil::writeExecutionPlan(*mgr);

  mgr->boot();

  mgr->start();
  mgr->join();

  mgr->start();
  mgr->join();

  auto tuples = collate->tuples();

  SPDLOG_DEBUG("Output:\n{}", tuples->toString());

	  CHECK(tuples->numRows() == 3);
	  CHECK(tuples->numColumns() == 3);

  mgr->stop();
}

}
