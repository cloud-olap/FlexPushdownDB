//
// Created by matt on 8/5/20.
//

#include <memory>
#include <vector>

#include <doctest/doctest.h>

#include <normal/expression/gandiva/Column.h>
#include <normal/expression/gandiva/LessThan.h>
#include <normal/expression/gandiva/Literal.h>
#include <normal/expression/gandiva/Cast.h>
#include <normal/expression/gandiva/Projector.h>
#include <normal/tuple/TupleSet2.h>
#include <normal/core/type/Integer32Type.h>

#include "Globals.h"
#include "TestUtil.h"

using namespace normal::tuple;
using namespace normal::core::type;
using namespace normal::expression::gandiva;
using namespace normal::expression::gandiva::test;

#define SKIP_SUITE false

TEST_SUITE ("binary-expression" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("less-than" * doctest::skip(false || SKIP_SUITE)) {

  auto tuples = TestUtil::prepareTupleSet();
  auto inputTupleSet = normal::tuple::TupleSet2::create(tuples);

  SPDLOG_DEBUG("Input:\n{}", inputTupleSet->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));

  auto expressions = std::vector<std::shared_ptr<normal::expression::gandiva::Expression>>{
	  lt(cast(col("a"), integer32Type()), lit<::arrow::Int32Type>(2)),
	  lt(cast(col("b"), integer32Type()), lit<::arrow::Int32Type>(5)),
	  lt(cast(col("c"), integer32Type()), lit<::arrow::Int32Type>(8)),
  };

  auto projector = std::make_shared<Projector>(expressions);
  projector->compile(tuples->table()->schema());

  SPDLOG_DEBUG("Projector:\n{}", projector->showString());

  auto evaluated = projector->evaluate(*tuples);
  auto evaluatedTupleSet = normal::tuple::TupleSet2::create(evaluated);
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