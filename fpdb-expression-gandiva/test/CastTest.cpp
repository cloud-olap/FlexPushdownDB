//
// Created by matt on 8/5/20.
//

#include <memory>
#include <vector>

#include <doctest/doctest.h>

#include <fpdb/expression/gandiva/Column.h>
#include <fpdb/expression/gandiva/Cast.h>
#include <fpdb/expression/gandiva/Projector.h>
#include <fpdb/tuple/TupleSet.h>

#include "Globals.h"
#include "TestUtil.h"

using namespace fpdb::tuple;
using namespace fpdb::expression::gandiva;
using namespace fpdb::expression::gandiva::test;

#define SKIP_SUITE true

TEST_SUITE ("cast" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("cast-string-to-decimal" * doctest::skip(false || SKIP_SUITE)) {

  auto inputTupleSet = TestUtil::prepareTupleSet();

  SPDLOG_DEBUG("Input:\n{}", inputTupleSet->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));

  auto expressions = std::vector<std::shared_ptr<fpdb::expression::gandiva::Expression>>{
	  cast(col("a"), arrow::decimal(38, 0)),
	  cast(col("b"), arrow::decimal(38, 0)),
	  cast(col("c"), arrow::decimal(38, 0))
  };

  auto projector = std::make_shared<Projector>(expressions);
  projector->compile(inputTupleSet->schema());

  SPDLOG_DEBUG("Projector:\n{}", projector->showString());

  auto evaluatedTupleSet = *projector->evaluate(*inputTupleSet);
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

  auto inputTupleSet = TestUtil::prepareTupleSet();

  SPDLOG_DEBUG("Input:\n{}", inputTupleSet->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));

  auto expressions = std::vector<std::shared_ptr<fpdb::expression::gandiva::Expression>>{
	  cast(col("a"), arrow::float64()),
	  cast(col("b"), arrow::float64()),
	  cast(col("c"), arrow::float64())
  };

  auto projector = std::make_shared<Projector>(expressions);
  projector->compile(inputTupleSet->schema());

  auto evaluatedTupleSet = *projector->evaluate(*inputTupleSet);
  SPDLOG_DEBUG("Output:\n{}", evaluatedTupleSet->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));

  auto value_a_0 = evaluatedTupleSet->value<arrow::DoubleType>("a", 0).value();
	  CHECK_EQ(value_a_0, 1.0);
  auto value_b_1 = evaluatedTupleSet->value<arrow::DoubleType>("b", 1).value();
	  CHECK_EQ(value_b_1, 5.0);
  auto value_c_2 = evaluatedTupleSet->value<arrow::DoubleType>("c", 2).value();
	  CHECK_EQ(value_c_2, 9.0);
}

TEST_CASE ("cast-string-to-long" * doctest::skip(false || SKIP_SUITE)) {

  auto inputTupleSet = TestUtil::prepareTupleSet();

  SPDLOG_DEBUG("Input:\n{}", inputTupleSet->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));

  auto expressions = std::vector<std::shared_ptr<fpdb::expression::gandiva::Expression>>{
	  cast(col("a"), arrow::int64()),
	  cast(col("b"), arrow::int64()),
	  cast(col("c"), arrow::int64())
  };

  auto projector = std::make_shared<Projector>(expressions);
  projector->compile(inputTupleSet->schema());

  auto evaluatedTupleSet = *projector->evaluate(*inputTupleSet);
  SPDLOG_DEBUG("Output:\n{}", evaluatedTupleSet->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));

  auto value_a_0 = evaluatedTupleSet->value<arrow::Int64Type>("a", 0).value();
	  CHECK_EQ(value_a_0, 1.0);
  auto value_b_1 = evaluatedTupleSet->value<arrow::Int64Type>("b", 1).value();
	  CHECK_EQ(value_b_1, 5.0);
  auto value_c_2 = evaluatedTupleSet->value<arrow::Int64Type>("c", 2).value();
	  CHECK_EQ(value_c_2, 9.0);
}

TEST_CASE ("cast-string-to-int" * doctest::skip(false || SKIP_SUITE)) {

  auto inputTupleSet = TestUtil::prepareTupleSet();

  SPDLOG_DEBUG("Input:\n{}", inputTupleSet->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));

  auto expressions = std::vector<std::shared_ptr<fpdb::expression::gandiva::Expression>>{
	  cast(col("a"), arrow::int32()),
	  cast(col("b"), arrow::int32()),
	  cast(col("c"), arrow::int32())
  };

  auto projector = std::make_shared<Projector>(expressions);
  projector->compile(inputTupleSet->schema());

  auto evaluatedTupleSet = *projector->evaluate(*inputTupleSet);
  SPDLOG_DEBUG("Output:\n{}", evaluatedTupleSet->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));

  auto value_a_0 = evaluatedTupleSet->value<arrow::Int32Type>("a", 0).value();
	  CHECK_EQ(value_a_0, 1.0);
  auto value_b_1 = evaluatedTupleSet->value<arrow::Int32Type>("b", 1).value();
	  CHECK_EQ(value_b_1, 5.0);
  auto value_c_2 = evaluatedTupleSet->value<arrow::Int32Type>("c", 2).value();
	  CHECK_EQ(value_c_2, 9.0);
}

}