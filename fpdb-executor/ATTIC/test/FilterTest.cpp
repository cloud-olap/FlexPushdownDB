//
// Created by matt on 6/5/20.
//


#include <memory>

#include <doctest/doctest.h>

#include <fpdb/pushdown/collate/Collate.h>
#include <fpdb/core/OperatorManager.h>
#include <fpdb/core/graph/OperatorGraph.h>
#include <fpdb/pushdown/file/FileScan.h>
#include <fpdb/pushdown/filter/Filter.h>
#include <fpdb/pushdown/filter/FilterPredicate.h>
#include <fpdb/expression/gandiva/Column.h>
#include <fpdb/expression/gandiva/NumericLiteral.h>
#include <fpdb/expression/gandiva/LessThan.h>
#include "TestUtil.h"

using namespace fpdb::pushdown;
using namespace fpdb::pushdown::test;
using namespace fpdb::pushdown::filter;
using namespace fpdb::tuple;
using namespace fpdb::expression::gandiva;
using namespace fpdb::core::graph;

#define SKIP_SUITE true

TEST_SUITE ("filter" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("filescan-filter-collate" * doctest::skip(false || SKIP_SUITE)) {

  auto aFile = filesystem::absolute("data/filter/a.csv");
  auto numBytesAFile = filesystem::file_size(aFile);

  auto mgr = std::make_shared<fpdb::core::OperatorManager>();

  auto g = OperatorGraph::make(mgr);

  auto scan = file::FileScan::make("fileScan", "data/filter/a.csv", std::vector<std::string>{"AA"}, 0, numBytesAFile, g->getId());
  auto filter = Filter::make("filter", FilterPredicate::make(lt(col("AA"), num_lit<::arrow::Int64Type>(11))));
  auto collate = std::make_shared<Collate>("collate", g->getId());

  scan->produce(filter);
  filter->consume(scan);

  filter->produce(collate);
  collate->consume(filter);

  g->put(scan);
  g->put(filter);
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
//	  CHECK(tupleSet->numRows() == 1);
//	  CHECK(tupleSet->numColumns() == 3);
//
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