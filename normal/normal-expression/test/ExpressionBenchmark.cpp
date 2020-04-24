//
// Created by matt on 24/4/20.
//

#include <doctest/doctest.h>
#include <nanobench.h>
#include <normal/core/type/Type.h>
#include <normal/expression/Expression.h>
#include <normal/core/arrow/Arrays.h>
#include <normal/core/TupleSet.h>
#include <normal/core/type/DecimalType.h>
#include <normal/expression/Column.h>
#include <normal/core/type/Float64Type.h>
#include <normal/expression/Cast.h>

#include <normal/expression/Projector.h>

using namespace normal::core::type;
using namespace normal::expression;

std::shared_ptr<normal::core::TupleSet> prepareTupleSet() {

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

  return tuples;
}

TEST_CASE ("benchmark-expression") {

  auto tuples = prepareTupleSet();

  auto expressions = std::vector<std::shared_ptr<normal::expression::Expression>>{
	  cast(col("a"), decimalType(10, 5)),
	  cast(col("b"), decimalType(10, 5)),
	  cast(col("c"), decimalType(10, 5))
  };

  auto projector = std::make_shared<Projector>(expressions);
  projector->compile(tuples->table()->schema());

  ankerl::nanobench::Config().run("evaluate-cast-to-decimal", [&] {
	auto projector = std::make_shared<Projector>(expressions);
	projector->compile(tuples->table()->schema());

	auto evaluated = tuples->evaluate(projector).value();
  });

  ankerl::nanobench::Config().run("evaluate-cast-to-decimal-reuse-projector", [&] {
	auto evaluated = tuples->evaluate(projector).value();
  });
}
