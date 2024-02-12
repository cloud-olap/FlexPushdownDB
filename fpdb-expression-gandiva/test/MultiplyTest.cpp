//
// Created by matt on 23/7/20.
//

#include <memory>
#include <vector>

#include <doctest/doctest.h>

#include <fpdb/expression/gandiva/Column.h>
#include <fpdb/expression/gandiva/Cast.h>
#include <fpdb/expression/gandiva/Multiply.h>
#include <fpdb/expression/gandiva/Projector.h>
#include <fpdb/tuple/TupleSet.h>

#include "Globals.h"
#include "TestUtil.h"

using namespace fpdb::tuple;
using namespace fpdb::expression::gandiva;
using namespace fpdb::expression::gandiva::test;

#define SKIP_SUITE true

TEST_SUITE ("multiply" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("simple" * doctest::skip(false || SKIP_SUITE)) {

  auto inputTupleSet = TestUtil::prepareTupleSet();

  SPDLOG_DEBUG("Input:\n{}", inputTupleSet->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));

  auto expressions = std::vector<std::shared_ptr<fpdb::expression::gandiva::Expression>>{
	  times(cast(col("a"), arrow::float64()),
			cast(col("b"), arrow::float64()))
  };

  auto projector = std::make_shared<Projector>(expressions);
  projector->compile(inputTupleSet->schema());

  SPDLOG_DEBUG("Projector:\n{}", projector->showString());

  auto evaluatedTupleSet = *projector->evaluate(*inputTupleSet);
  SPDLOG_DEBUG("Output:\n{}", evaluatedTupleSet->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));

	  CHECK_EQ(evaluatedTupleSet->numColumns(), 1);
	  CHECK_EQ(evaluatedTupleSet->numRows(), 3);

  auto column = evaluatedTupleSet->getColumnByIndex(0).value();
	  CHECK_EQ(column->element(0).value()->value<double>(), 4);
	  CHECK_EQ(column->element(1).value()->value<double>(), 10);
	  CHECK_EQ(column->element(2).value()->value<double>(), 18);
}

TEST_CASE ("empty" * doctest::skip(false || SKIP_SUITE)) {

  auto inputTupleSet = TestUtil::prepareEmptyTupleSet();

  SPDLOG_DEBUG("Input:\n{}", inputTupleSet->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));

  auto expressions = std::vector<std::shared_ptr<fpdb::expression::gandiva::Expression>>{
	  times(cast(col("a"), arrow::float64()),
			cast(col("b"), arrow::float64()))
  };

  auto projector = std::make_shared<Projector>(expressions);
  CHECK_THROWS(projector->compile(inputTupleSet->schema()));
}

TEST_CASE ("0-rows" * doctest::skip(false || SKIP_SUITE)) {

  auto inputTupleSet = TestUtil::prepare3x0TupleSet();

  SPDLOG_DEBUG("Input:\n{}", inputTupleSet->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));

  auto expressions = std::vector<std::shared_ptr<fpdb::expression::gandiva::Expression>>{
	  times(cast(col("a"), arrow::float64()),
			cast(col("b"), arrow::float64()))
  };

  auto projector = std::make_shared<Projector>(expressions);
  projector->compile(inputTupleSet->schema());

  SPDLOG_DEBUG("Projector:\n{}", projector->showString());

  auto evaluatedTupleSet = *projector->evaluate(*inputTupleSet);
  SPDLOG_DEBUG("Output:\n{}", evaluatedTupleSet->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));

	  CHECK_EQ(evaluatedTupleSet->numColumns(), 1);
	  CHECK_EQ(evaluatedTupleSet->numRows(), 0);
}

}
