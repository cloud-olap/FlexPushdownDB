//
// Created by Jialing Pei on 5/13/20.
//

#include <memory>

#include <doctest/doctest.h>

#include <fpdb/pushdown/collate/Collate.h>
#include <fpdb/core/OperatorManager.h>
#include <fpdb/core/graph/OperatorGraph.h>
#include <fpdb/pushdown/file/FileScan.h>
#include <fpdb/expression/gandiva/Column.h>
#include <fpdb/pushdown/ATTIC/Sort.h>
#include <fpdb/core/type/Integer32Type.h>
#include <fpdb/expression/gandiva/Cast.h>
#include <fpdb/tuple/TupleSet2.h>
#include "TestUtil.h"

using namespace fpdb::pushdown;
using namespace fpdb::pushdown::test;
using namespace fpdb::tuple;
using namespace fpdb::expression::gandiva;
using namespace fpdb::core::graph;

#define SKIP_SUITE true

TEST_SUITE ("sort" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("SortTest" * doctest::skip(false || SKIP_SUITE)) {

  auto aFile = filesystem::absolute("data/filter/a.csv");
  auto numBytesAFile = filesystem::file_size(aFile);

  auto mgr = std::make_shared<fpdb::core::OperatorManager>();

  auto g = OperatorGraph::make(mgr);

  auto scan = file::FileScan::make("fileScan", "data/filter/a.csv", std::vector<std::string>{"aa"}, 0, numBytesAFile, g->getId(), true);
  auto collate = std::make_shared<Collate>("collate", g->getId());
  std::shared_ptr<std::vector<int>> priorities = std::make_shared<std::vector<int>>(std::vector<int>{0});
  auto sort = std::make_shared<Sort>("sort", cast(col("aa"), fpdb::core::type::integer32Type()), priorities);
  scan->produce(sort);
  sort->consume(scan);

  sort->produce(collate);
  collate->consume(sort);

  g->put(scan);
  g->put(sort);
  g->put(collate);

  TestUtil::writeExecutionPlan(*g);

  mgr->boot();

  mgr->start();
//  mgr->join();
//
//  auto tuples = collate->tuples();

  mgr->stop();

//  auto tupleSet = TupleSet2::create(tuples);
//
//  SPDLOG_INFO("Output:\n{}", tupleSet->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));
//
//	  CHECK(tupleSet->numRows() == 3);
//	  CHECK(tupleSet->numColumns() == 3);
//
//    /*
//     * FIXME: The following assumes the output is produced in a specific order but this shouldn't necessarily
//     *  be assumed. Will only be able to check the properly once we have a sort operator
//     */
//    auto columnAA = tupleSet->getColumnByName("AA").value();
//            CHECK(columnAA->element(0).value()->value<long>() == 10);
//
//    auto columnAB = tupleSet->getColumnByName("AB").value();
//            CHECK(columnAB->element(0).value()->value<long>() == 13);
//
//    auto columnAC = tupleSet->getColumnByName("AC").value();
//            CHECK(columnAC->element(0).value()->value<long>() == 16);

}

}
