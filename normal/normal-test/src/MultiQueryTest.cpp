//
// Created by matt on 5/3/20.
//

#include <memory>

#include <doctest/doctest.h>

#include "normal/pushdown/Collate.h"
#include <normal/core/OperatorManager.h>
#include <normal/pushdown/FileScan.h>
#include <normal/test/TestUtil.h>
#include "normal/test/Globals.h"

/**
 * Test to make sure a query can be run and then re run (useful for instances such as
 * warming up a cache on the first run)
 */
TEST_CASE ("FileScan -> Collate"
               * doctest::skip(false)) {

  auto mgr = std::make_shared<normal::core::OperatorManager>();

  auto fileScan = std::make_shared<normal::pushdown::FileScan>("fileScan", "data/data-file-simple/test.csv");
  auto collate = std::make_shared<normal::pushdown::Collate>("collate");

  fileScan->produce(collate);
  collate->consume(fileScan);

  mgr->put(fileScan);
  mgr->put(collate);

  normal::test::TestUtil::writeLogicalExecutionPlan(*mgr);

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