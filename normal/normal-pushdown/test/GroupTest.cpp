//
// Created by matt on 13/5/20.
//

#include <memory>

#include <doctest/doctest.h>

#include <normal/pushdown/Collate.h>
#include <normal/core/OperatorManager.h>
#include <normal/core/graph/OperatorGraph.h>
#include <normal/pushdown/file/FileScan.h>
#include <normal/pushdown/group/Group.h>
#include <normal/tuple/TupleSet2.h>
#include <normal/pushdown/aggregate/Sum.h>
#include <normal/expression/gandiva/Column.h>
#include <normal/core/type/Float64Type.h>
#include <normal/expression/gandiva/Cast.h>
#include "TestUtil.h"

using namespace normal::pushdown;
using namespace normal::pushdown::test;
using namespace normal::pushdown::group;
using namespace normal::tuple;
using namespace normal::core::type;
using namespace normal::core::graph;
using namespace normal::expression;
using namespace normal::expression::gandiva;

#define SKIP_SUITE true

TEST_SUITE ("group" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("group" * doctest::skip(false || SKIP_SUITE)) {

  auto aFile = filesystem::absolute("data/group/a.csv");
  auto numBytesAFile = filesystem::file_size(aFile);

  auto mgr = std::make_shared<normal::core::OperatorManager>();

  auto g = OperatorGraph::make(mgr);

  auto scan = FileScan::make("fileScan", "data/group/a.csv", std::vector<std::string>{"AA", "AB"}, 0, numBytesAFile, g->getId());
  auto sumExpr = std::make_shared<normal::pushdown::aggregate::Sum>("sum",
																	cast(col("AB"), float64Type()));
  auto
	  expressions2 = std::make_shared<std::vector<std::shared_ptr<normal::pushdown::aggregate::AggregationFunction>>>();
  expressions2->push_back(sumExpr);

  auto group = Group::make("group", {"AA"}, expressions2);
  auto collate = std::make_shared<Collate>("collate", g->getId());

  scan->produce(group);
  group->consume(scan);

  group->produce(collate);
  collate->consume(group);

  mgr->put(scan);
  mgr->put(group);
  mgr->put(collate);

  TestUtil::writeExecutionPlan(*g);

  mgr->boot();

  mgr->start();
  mgr->join();

  auto tuples = collate->tuples();

  mgr->stop();

  auto tupleSet = TupleSet2::create(tuples);

  SPDLOG_INFO("Output:\n{}", tupleSet->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));

	  CHECK(tupleSet->numRows() == 2);
	  CHECK(tupleSet->numColumns() == 2);

  /*
   * FIXME: The following assumes the output is produced in a specific order but this shouldn't necessarily
   *  be assumed. Will only be able to check properly once we have a sort operator
   */
  auto columnAA = tupleSet->getColumnByName("AA").value();
	  CHECK(columnAA->element(0).value()->value<long>() == 10);
	  CHECK(columnAA->element(1).value()->value<long>() == 12);

  auto columnAB = tupleSet->getColumnByName("sum").value();
	  CHECK(columnAB->element(0).value()->value<double>() == 27);
	  CHECK(columnAB->element(1).value()->value<double>() == 15);

}

}