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

TEST_CASE ("Make" * doctest::skip(true)) {

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

TEST_CASE ("Expression" * doctest::skip(false)) {

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
      cast(col("a"), decimalType(5, 2)),
      cast(col("b"), decimalType(5, 2)),
      cast(col("c"), decimalType(5, 2))
  };

  auto evaluated = tuples->evaluate(expressions);

  SPDLOG_DEBUG("Output:\n{}", evaluated.value()->toString());
}
