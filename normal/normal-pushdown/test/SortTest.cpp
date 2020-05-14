//
// Created by Jialing Pei on 5/13/20.
//

#include <memory>

#include <doctest/doctest.h>

#include <normal/pushdown/Collate.h>
#include <normal/core/OperatorManager.h>
#include <normal/pushdown/FileScan.h>
#include <normal/test/TestUtil.h>
#include <normal/expression/gandiva/Column.h>
#include <normal/pushdown/Sort.h>
#include <normal/core/type/Integer32Type.h>
#include <normal/expression/gandiva/Cast.h>
#include <normal/tuple/TupleSet2.h>

using namespace normal::pushdown;
using namespace normal::tuple;
using namespace normal::expression::gandiva;

#define SKIP_SUITE true

TEST_SUITE ("aggregate" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("SortTest" * doctest::skip(false || SKIP_SUITE)) {

  auto mgr = std::make_shared<normal::core::OperatorManager>();

  auto scan = std::make_shared<FileScan>("fileScan", "data/filter/a.csv");
  auto collate = std::make_shared<Collate>("collate");
  std::shared_ptr<std::vector<int>> priorities = std::make_shared<std::vector<int>>(std::vector<int>{0});
  auto sort = std::make_shared<Sort>("sort", cast(col("aa"), normal::core::type::integer32Type()), priorities);
  scan->produce(sort);
  sort->consume(scan);

  sort->produce(collate);
  collate->consume(sort);

  mgr->put(scan);
  mgr->put(sort);
  mgr->put(collate);

  normal::test::TestUtil::writeExecutionPlan(*mgr);

  mgr->boot();

  mgr->start();
  mgr->join();

  auto tuples = collate->tuples();

  mgr->stop();

  auto tupleSet = TupleSet2::create(tuples);

  SPDLOG_INFO("Output:\n{}", tupleSet->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));

	  CHECK(tupleSet->numRows() == 3);
	  CHECK(tupleSet->numColumns() == 3);

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
