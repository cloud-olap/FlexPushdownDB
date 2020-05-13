//
// Created by matt on 13/5/20.
//

#include <memory>

#include <doctest/doctest.h>

#include <normal/pushdown/Collate.h>
#include <normal/core/OperatorManager.h>
#include <normal/pushdown/FileScan.h>
#include <normal/test/TestUtil.h>
#include <normal/pushdown/group/Group.h>
#include <normal/tuple/TupleSet2.h>
#include <normal/pushdown/aggregate/Sum.h>
#include <normal/expression/gandiva/Column.h>
#include <normal/core/type/Float64Type.h>
#include <normal/expression/gandiva/Cast.h>

using namespace normal::pushdown;
using namespace normal::pushdown::group;
using namespace normal::tuple;
using namespace normal::core::type;
using namespace normal::expression;
using namespace normal::expression::gandiva;

#define SKIP_SUITE true

TEST_SUITE ("group" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("filescan-group-collate" * doctest::skip(false || SKIP_SUITE)) {

  auto mgr = std::make_shared<normal::core::OperatorManager>();

  auto scan = std::make_shared<FileScan>("fileScan", "data/filter/a.csv");
  auto sumExpr = std::make_shared<normal::pushdown::aggregate::Sum>("sum",
																	cast(col("AB"), float64Type()));
  auto
	  expressions2 = std::make_shared<std::vector<std::shared_ptr<normal::pushdown::aggregate::AggregationFunction>>>();
  expressions2->push_back(sumExpr);

  auto group = Group::make("group", {"AA"}, expressions2);
  auto collate = std::make_shared<Collate>("collate");

  scan->produce(group);
  group->consume(scan);

  group->produce(collate);
  collate->consume(group);

  mgr->put(scan);
  mgr->put(group);
  mgr->put(collate);

  normal::test::TestUtil::writeExecutionPlan(*mgr);

  mgr->boot();

  mgr->start();
  mgr->join();

  auto tuples = collate->tuples();

  mgr->stop();

  auto tupleSet = TupleSet2::create(tuples);

  SPDLOG_INFO("Output:\n{}", tupleSet->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));

	  CHECK(tupleSet->numRows() == 1);
	  CHECK(tupleSet->numColumns() == 3);

//  /*
//   * FIXME: The following assumes the output is produced in a specific order but this shouldn't necessarily
//   *  be assumed. Will only be able to check the properly once we have a sort operator
//   */
//  auto columnAA = tupleSet->getColumnByName("AA").value();
//	  CHECK(columnAA->element(0).value()->value<long>() == 10);
//
//  auto columnAB = tupleSet->getColumnByName("AB").value();
//	  CHECK(columnAB->element(0).value()->value<long>() == 13);
//
//  auto columnAC = tupleSet->getColumnByName("AC").value();
//	  CHECK(columnAC->element(0).value()->value<long>() == 16);

}

}