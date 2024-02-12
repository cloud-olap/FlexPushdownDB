//
// Created by matt on 8/5/20.
//

#include <memory>
#include <vector>

#include <doctest/doctest.h>

#include <fpdb/expression/gandiva/Column.h>
#include <fpdb/expression/gandiva/LessThan.h>
#include <fpdb/expression/gandiva/NumericLiteral.h>
#include <fpdb/expression/gandiva/Cast.h>
#include <fpdb/expression/gandiva/Projector.h>
#include <fpdb/tuple/TupleSet.h>

#include "Globals.h"
#include "TestUtil.h"

using namespace fpdb::tuple;
using namespace fpdb::expression::gandiva;
using namespace fpdb::expression::gandiva::test;

#define SKIP_SUITE true

TEST_SUITE ("binary-expression" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("less-than" * doctest::skip(false || SKIP_SUITE)) {

  auto inputTupleSet = TestUtil::prepareTupleSet();

  SPDLOG_DEBUG("Input:\n{}", inputTupleSet->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));

  auto expressions = std::vector<std::shared_ptr<fpdb::expression::gandiva::Expression>>{
	  lt(cast(col("a"), arrow::int32()), num_lit<::arrow::Int32Type>(std::optional<int>(2))),
	  lt(cast(col("b"), arrow::int32()), num_lit<::arrow::Int32Type>(std::optional<int>(5))),
	  lt(cast(col("c"), arrow::int32()), num_lit<::arrow::Int32Type>(std::optional<int>(8))),
  };

  auto projector = std::make_shared<Projector>(expressions);
  projector->compile(inputTupleSet->schema());

  SPDLOG_DEBUG("Projector:\n{}", projector->showString());

  auto evaluatedTupleSet = *projector->evaluate(*inputTupleSet);
  SPDLOG_DEBUG("Output:\n{}", evaluatedTupleSet->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));

  auto columnA = evaluatedTupleSet->getColumnByIndex(0).value();
	  CHECK_EQ(columnA->element(0).value()->value<bool>(), true);
	  CHECK_EQ(columnA->element(1).value()->value<bool>(), false);
	  CHECK_EQ(columnA->element(2).value()->value<bool>(), false);

  auto columnB = evaluatedTupleSet->getColumnByIndex(1).value();
	  CHECK_EQ(columnB->element(0).value()->value<bool>(), true);
	  CHECK_EQ(columnB->element(1).value()->value<bool>(), false);
	  CHECK_EQ(columnB->element(2).value()->value<bool>(), false);

  auto columnC = evaluatedTupleSet->getColumnByIndex(2).value();
	  CHECK_EQ(columnC->element(0).value()->value<bool>(), true);
	  CHECK_EQ(columnC->element(1).value()->value<bool>(), false);
	  CHECK_EQ(columnC->element(2).value()->value<bool>(), false);

}

}