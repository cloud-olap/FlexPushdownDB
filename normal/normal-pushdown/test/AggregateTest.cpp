//
// Created by matt on 13/5/20.
//

#include <memory>

#include <doctest/doctest.h>

#include <normal/pushdown/Collate.h>
#include <normal/core/OperatorManager.h>
#include <normal/pushdown/file/FileScan.h>
#include <normal/test/TestUtil.h>
#include <normal/tuple/TupleSet2.h>
#include <normal/pushdown/aggregate/Sum.h>
#include <normal/expression/gandiva/Column.h>
#include <normal/core/type/Float64Type.h>
#include <normal/expression/gandiva/Cast.h>
#include <normal/pushdown/Aggregate.h>

using namespace normal::pushdown;
using namespace normal::pushdown::aggregate;
using namespace normal::tuple;
using namespace normal::core::type;
using namespace normal::expression;
using namespace normal::expression::gandiva;

#define SKIP_SUITE true

TEST_SUITE ("aggregate" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("sum" * doctest::skip(false || SKIP_SUITE)) {

  auto mgr = std::make_shared<normal::core::OperatorManager>();

  auto fileScan = std::make_shared<normal::pushdown::FileScan>("fileScan", "data/aggregate/a.csv");
  auto aggregateFunctions = std::make_shared<std::vector<std::shared_ptr<AggregationFunction>>>();
  aggregateFunctions->
	  emplace_back(std::make_shared<Sum>("Sum", cast(col("AA"), float64Type()))
  );
  auto aggregate = std::make_shared<normal::pushdown::Aggregate>("aggregate", aggregateFunctions);
  auto collate = std::make_shared<normal::pushdown::Collate>("collate");

  fileScan->produce(aggregate);
  aggregate->consume(fileScan);

  aggregate->produce(collate);
  collate->consume(aggregate);

  mgr->put(fileScan);
  mgr->put(aggregate);
  mgr->put(collate);

  normal::test::TestUtil::writeExecutionPlan(*mgr);

  mgr->boot();

  mgr->start();
  mgr->join();

  auto tuples = collate->tuples();

  auto val = tuples->value<arrow::DoubleType>("Sum", 0);

	  CHECK(tuples->numRows() == 1);
	  CHECK(tuples->numColumns() == 1);
	  CHECK(val == 33.0);

  mgr->stop();

}

}