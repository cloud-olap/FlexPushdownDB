//
// Created by matt on 5/3/20.
//

#include <memory>

#include <doctest/doctest.h>

#include <normal/pushdown/Collate.h>
#include <normal/core/OperatorManager.h>
#include <normal/pushdown/file/FileScan.h>
#include <normal/test/TestUtil.h>

using namespace normal::pushdown;
using namespace normal::tuple;
using namespace normal::core;
using namespace normal::test;

#define SKIP_SUITE false

/**
 * Test running a query multiple times to test cache hits on second run
 */
TEST_SUITE ("cache" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("multi-filescan-collate" * doctest::skip(false)) {

  auto mgr = std::make_shared<OperatorManager>();

  auto fileScan = FileScan::make("fileScan",
								 "data/cache/test.csv",
								 {"a", "b", "c"},
								 0,
								 1023);
  auto collate = std::make_shared<Collate>("collate");

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
