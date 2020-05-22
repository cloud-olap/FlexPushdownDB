//
// Created by matt on 8/5/20.
//

#include <memory>
#include <vector>

#include <doctest/doctest.h>

#include <normal/core/type/DecimalType.h>
#include <normal/expression/gandiva/Column.h>
#include <normal/core/type/Float64Type.h>
#include <normal/expression/gandiva/Cast.h>
#include <normal/expression/Projector.h>
#include <normal/expression/gandiva/Projector.h>
#include <normal/tuple/TupleSet2.h>
#include <normal/core/type/Integer32Type.h>
#include <normal/core/type/Integer64Type.h>

#include "Globals.h"
#include "TestUtil.h"

using namespace normal::tuple;
using namespace normal::core::type;
using namespace normal::expression::gandiva;
using namespace normal::expression::gandiva::test;

#define SKIP_SUITE false

TEST_SUITE ("cast" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("cast-string-to-decimal" * doctest::skip(false || SKIP_SUITE)) {

  auto tuples = TestUtil::prepareTupleSet();
  auto inputTupleSet = normal::tuple::TupleSet2::create(tuples);

  SPDLOG_DEBUG("Input:\n{}", inputTupleSet->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));

  auto expressions = std::vector<std::shared_ptr<normal::expression::gandiva::Expression>>{
	  cast(col("a"), decimalType(38, 0)),
	  cast(col("b"), decimalType(38, 0)),
	  cast(col("c"), decimalType(38, 0))
  };

  auto projector = std::make_shared<Projector>(expressions);
  projector->compile(tuples->table()->schema());

  SPDLOG_DEBUG("Projector:\n{}", projector->showString());

  auto evaluated = projector->evaluate(*tuples);
  auto evaluatedTupleSet = normal::tuple::TupleSet2::create(evaluated);
  SPDLOG_DEBUG("Output:\n{}", evaluatedTupleSet->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));

  auto columnA = evaluatedTupleSet->getColumnByIndex(0).value();
	  CHECK_EQ(columnA->element(0).value()->value<::arrow::Decimal128>(), ::arrow::Decimal128(1));
	  CHECK_EQ(columnA->element(1).value()->value<::arrow::Decimal128>(), ::arrow::Decimal128(2));
	  CHECK_EQ(columnA->element(2).value()->value<::arrow::Decimal128>(), ::arrow::Decimal128(3));

  auto columnB = evaluatedTupleSet->getColumnByIndex(1).value();
	  CHECK_EQ(columnB->element(0).value()->value<::arrow::Decimal128>(), ::arrow::Decimal128(4));
	  CHECK_EQ(columnB->element(1).value()->value<::arrow::Decimal128>(), ::arrow::Decimal128(5));
	  CHECK_EQ(columnB->element(2).value()->value<::arrow::Decimal128>(), ::arrow::Decimal128(6));

  auto columnC = evaluatedTupleSet->getColumnByIndex(2).value();
	  CHECK_EQ(columnC->element(0).value()->value<::arrow::Decimal128>(), ::arrow::Decimal128(7));
	  CHECK_EQ(columnC->element(1).value()->value<::arrow::Decimal128>(), ::arrow::Decimal128(8));
	  CHECK_EQ(columnC->element(2).value()->value<::arrow::Decimal128>(), ::arrow::Decimal128(9));
}

TEST_CASE ("cast-string-to-double" * doctest::skip(false || SKIP_SUITE)) {

  auto tuples = TestUtil::prepareTupleSet();
  auto inputTupleSet = normal::tuple::TupleSet2::create(tuples);

  SPDLOG_DEBUG("Input:\n{}", inputTupleSet->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));

  auto expressions = std::vector<std::shared_ptr<normal::expression::gandiva::Expression>>{
	  cast(col("a"), float64Type()),
	  cast(col("b"), float64Type()),
	  cast(col("c"), float64Type())
  };

  auto projector = std::make_shared<Projector>(expressions);
  projector->compile(tuples->table()->schema());

  auto evaluated = projector->evaluate(*tuples);
  auto evaluatedTupleSet = normal::tuple::TupleSet2::create(evaluated);
  SPDLOG_DEBUG("Output:\n{}", evaluatedTupleSet->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));

  auto value_a_0 = evaluated->value<arrow::DoubleType>("a", 0).value();
	  CHECK_EQ(value_a_0, 1.0);
  auto value_b_1 = evaluated->value<arrow::DoubleType>("b", 1).value();
	  CHECK_EQ(value_b_1, 5.0);
  auto value_c_2 = evaluated->value<arrow::DoubleType>("c", 2).value();
	  CHECK_EQ(value_c_2, 9.0);
}

TEST_CASE ("cast-string-to-long" * doctest::skip(false || SKIP_SUITE)) {

  auto tuples = TestUtil::prepareTupleSet();
  auto inputTupleSet = normal::tuple::TupleSet2::create(tuples);

  SPDLOG_DEBUG("Input:\n{}", inputTupleSet->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));

  auto expressions = std::vector<std::shared_ptr<normal::expression::gandiva::Expression>>{
	  cast(col("a"), integer64Type()),
	  cast(col("b"), integer64Type()),
	  cast(col("c"), integer64Type())
  };

  auto projector = std::make_shared<Projector>(expressions);
  projector->compile(tuples->table()->schema());

  auto evaluated = projector->evaluate(*tuples);
  auto evaluatedTupleSet = normal::tuple::TupleSet2::create(evaluated);
  SPDLOG_DEBUG("Output:\n{}", evaluatedTupleSet->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));

  auto value_a_0 = evaluated->value<arrow::Int64Type>("a", 0).value();
	  CHECK_EQ(value_a_0, 1.0);
  auto value_b_1 = evaluated->value<arrow::Int64Type>("b", 1).value();
	  CHECK_EQ(value_b_1, 5.0);
  auto value_c_2 = evaluated->value<arrow::Int64Type>("c", 2).value();
	  CHECK_EQ(value_c_2, 9.0);
}

TEST_CASE ("cast-string-to-int" * doctest::skip(false || SKIP_SUITE)) {

  auto tuples = TestUtil::prepareTupleSet();
  auto inputTupleSet = normal::tuple::TupleSet2::create(tuples);

  SPDLOG_DEBUG("Input:\n{}", inputTupleSet->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));

  auto expressions = std::vector<std::shared_ptr<normal::expression::gandiva::Expression>>{
	  cast(col("a"), integer32Type()),
	  cast(col("b"), integer32Type()),
	  cast(col("c"), integer32Type())
  };

  auto projector = std::make_shared<Projector>(expressions);
  projector->compile(tuples->table()->schema());

 auto evaluated = projector->evaluate(*tuples);
  auto evaluatedTupleSet = normal::tuple::TupleSet2::create(evaluated);
  SPDLOG_DEBUG("Output:\n{}", evaluatedTupleSet->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));

  auto value_a_0 = evaluated->value<arrow::Int32Type>("a", 0).value();
	  CHECK_EQ(value_a_0, 1.0);
  auto value_b_1 = evaluated->value<arrow::Int32Type>("b", 1).value();
	  CHECK_EQ(value_b_1, 5.0);
  auto value_c_2 = evaluated->value<arrow::Int32Type>("c", 2).value();
	  CHECK_EQ(value_c_2, 9.0);
}

}