//
// Created by matt on 13/5/20.
//

#include <memory>

#include <doctest/doctest.h>

#include <fpdb/pushdown/collate/Collate.h>
#include <fpdb/core/OperatorManager.h>
#include <fpdb/core/graph/OperatorGraph.h>
#include <fpdb/pushdown/file/FileScan.h>
#include <fpdb/tuple/TupleSet2.h>
#include <fpdb/pushdown/aggregate/Sum.h>
#include <fpdb/expression/gandiva/Column.h>
#include <fpdb/core/type/Float64Type.h>
#include <fpdb/expression/gandiva/Cast.h>
#include <fpdb/pushdown/aggregate/Aggregate.h>
#include "TestUtil.h"

using namespace fpdb::pushdown;
using namespace fpdb::pushdown::test;
using namespace fpdb::pushdown::aggregate;
using namespace fpdb::tuple;
using namespace fpdb::core::type;
using namespace fpdb::core::graph;
using namespace fpdb::expression;
using namespace fpdb::expression::gandiva;

#define SKIP_SUITE true

TEST_SUITE ("aggregate" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("sum" * doctest::skip(false || SKIP_SUITE)) {

  auto aFile = filesystem::absolute("data/aggregate/a.csv");
  auto numBytesAFile = filesystem::file_size(aFile);

  auto mgr = std::make_shared<OperatorManager>();
  mgr->boot();
  mgr->start();

  auto g = OperatorGraph::make(mgr);

  auto fileScan = file::FileScan::make("fileScan",
								 "data/aggregate/a.csv",
								 std::vector<std::string>{"AA"},
								 0,
								 numBytesAFile,
								 g->getId(),
								 true);
  auto aggregateFunctions = std::make_shared<std::vector<std::shared_ptr<AggregationFunction>>>();
  aggregateFunctions->
	  emplace_back(std::make_shared<Sum>("sum", cast(col("AA"), float64Type()))
  );
  auto aggregate = std::make_shared<Aggregate>("aggregate", aggregateFunctions);
  auto collate = std::make_shared<Collate>("collate", g->getId());

  fileScan->produce(aggregate);
  aggregate->consume(fileScan);

  aggregate->produce(collate);
  collate->consume(aggregate);

  g->put(fileScan);
  g->put(aggregate);
  g->put(collate);

  TestUtil::writeExecutionPlan(*g);

  g->boot();

  g->start();
  g->join();

  auto tuples = collate->tuples();

  auto val = tuples->value<arrow::DoubleType>("Sum", 0);

	  CHECK(tuples->numRows() == 1);
	  CHECK(tuples->numColumns() == 1);
	  CHECK_EQ(33.0, val.value());

  mgr->stop();

}

}