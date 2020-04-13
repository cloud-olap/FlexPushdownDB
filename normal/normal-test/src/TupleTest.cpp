//
// Created by matt on 2/4/20.
//

#include <doctest/doctest.h>

#include <normal/core/arrow/Arrays.h>
#include <normal/core/expression/Expression.h>
#include <normal/core/expression/Cast.h>
#include <normal/core/expression/Column.h>
#include <normal/core/TupleSet.h>
#include <normal/core/type/Type.h>
#include <normal/core/type/DecimalType.h>

#include "gandiva/arrow.h"

#include "Globals.h"

using namespace normal::core::type;
using namespace normal::core::expression;

TEST_CASE ("make-tupleset" * doctest::skip(false)) {

  auto column1 = std::vector{"1", "2", "3"};
  auto column2 = std::vector{"4", "5", "6"};
  auto column3 = std::vector{"7", "8", "9"};

  auto stringType = arrow::TypeTraits<arrow::StringType>::type_singleton();

  auto fieldA = field("a", stringType);
  auto fieldB = field("b", stringType);
  auto fieldC = field("c", stringType);
  auto schema = arrow::schema({fieldA, fieldB, fieldC});

  auto arrowColumn1 = Arrays::make<arrow::StringType>(column1).value();
  auto arrowColumn2 = Arrays::make<arrow::StringType>(column2).value();
  auto arrowColumn3 = Arrays::make<arrow::StringType>(column3).value();

  auto tuples = normal::core::TupleSet::make(schema, {arrowColumn1, arrowColumn2, arrowColumn3});

  SPDLOG_DEBUG("Output:\n{}", tuples->toString());
}

TEST_CASE ("cast-string-to-decimal" * doctest::skip(false)) {

  auto column1 = std::vector{"1", "2", "3"};
  auto column2 = std::vector{"4", "5", "6"};
  auto column3 = std::vector{"7", "8", "9"};

  auto stringType = arrow::TypeTraits<arrow::StringType>::type_singleton();

  auto fieldA = field("a", stringType);
  auto fieldB = field("b", stringType);
  auto fieldC = field("c", stringType);
  auto schema = arrow::schema({fieldA, fieldB, fieldC});

  auto arrowColumn1 = Arrays::make<arrow::StringType>(column1).value();
  auto arrowColumn2 = Arrays::make<arrow::StringType>(column2).value();
  auto arrowColumn3 = Arrays::make<arrow::StringType>(column3).value();

  auto tuples = normal::core::TupleSet::make(schema, {arrowColumn1, arrowColumn2, arrowColumn3});

  SPDLOG_DEBUG("Input:\n{}", tuples->toString());

  auto expressions = std::vector<std::shared_ptr<normal::core::expression::Expression>>{
      cast(col("a"), decimalType(10, 5)),
      cast(col("b"), decimalType(10, 5)),
      cast(col("c"), decimalType(10, 5))
  };

  auto evaluated = tuples->evaluate(expressions).value();
  SPDLOG_DEBUG("Output:\n{}", evaluated->toString());
}

TEST_CASE ("cast-string-to-double" * doctest::skip(false)) {

  auto column1 = std::vector{"1", "2", "3"};
  auto column2 = std::vector{"4", "5", "6"};
  auto column3 = std::vector{"7", "8", "9"};

  auto stringType = arrow::TypeTraits<arrow::StringType>::type_singleton();

  auto fieldA = field("a", stringType);
  auto fieldB = field("b", stringType);
  auto fieldC = field("c", stringType);
  auto schema = arrow::schema({fieldA, fieldB, fieldC});

  auto arrowColumn1 = Arrays::make<arrow::StringType>(column1).value();
  auto arrowColumn2 = Arrays::make<arrow::StringType>(column2).value();
  auto arrowColumn3 = Arrays::make<arrow::StringType>(column3).value();

  auto tuples = normal::core::TupleSet::make(schema, {arrowColumn1, arrowColumn2, arrowColumn3});

  SPDLOG_DEBUG("Input:\n{}", tuples->toString());

  auto expressions = std::vector<std::shared_ptr<normal::core::expression::Expression>>{
      cast(col("a"), float64Type()),
      cast(col("b"), float64Type()),
      cast(col("c"), float64Type())
  };

  auto evaluated = tuples->evaluate(expressions).value();
  SPDLOG_DEBUG("Output:\n{}", evaluated->toString());

  auto value_a_0 = evaluated->value<arrow::DoubleType>("a", 0).value();
      CHECK_EQ(value_a_0, 1.0);
  auto value_b_1 = evaluated->value<arrow::DoubleType>("b", 1).value();
      CHECK_EQ(value_b_1, 5.0);
  auto value_c_2 = evaluated->value<arrow::DoubleType>("c", 2).value();
      CHECK_EQ(value_c_2, 9.0);
}
