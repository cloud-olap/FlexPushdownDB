//
// Created by matt on 20/5/20.
//

#include <doctest/doctest.h>

#include <normal/core/arrow/Arrays.h>
#include <normal/core/TupleSet.h>
#include <normal/core/type/DecimalType.h>

using namespace normal::core::type;
using namespace normal::expression;

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
