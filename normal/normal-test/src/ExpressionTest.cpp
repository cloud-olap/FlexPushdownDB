//
// Created by matt on 20/4/20.
//

#include <memory>
#include <vector>

#include <doctest/doctest.h>
#include <gandiva/function_registry.h>

#include <normal/core/type/Type.h>
#include <normal/expression/Expression.h>
#include <normal/core/arrow/Arrays.h>
#include <normal/core/TupleSet.h>
#include <normal/core/type/DecimalType.h>
#include <normal/expression/gandiva/Column.h>
#include <normal/core/type/Float64Type.h>
#include <normal/expression/gandiva/Cast.h>

#include <normal/test/Globals.h>
#include <normal/test/TestUtil.h>
#include <normal/expression/Projector.h>
#include <normal/expression/gandiva/Projector.h>

using namespace normal::core::type;
using namespace normal::expression::gandiva;

/**
 * Create a simple 3 col, 3 row tupleset for testing expressions against
 *
 * @return
 */
std::shared_ptr<normal::core::TupleSet> prepareTupleSet(){

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

TEST_CASE ("cast-string-to-decimal" * doctest::skip(false)) {

	auto tuples = prepareTupleSet();

	SPDLOG_DEBUG("Input:\n{}", tuples->toString());

	auto expressions = std::vector<std::shared_ptr<normal::expression::gandiva::Expression>>{
		cast(col("a"), decimalType(10, 5)),
		cast(col("b"), decimalType(10, 5)),
		cast(col("c"), decimalType(10, 5))
	};

	auto projector = std::make_shared<Projector>(expressions);
	projector->compile(tuples->table()->schema());

  	SPDLOG_DEBUG("Projector:\n{}", projector->showString());

	auto evaluated = tuples->evaluate(projector).value();
	SPDLOG_DEBUG("Output:\n{}", evaluated->toString());
}

TEST_CASE ("show-gandiva-functions" * doctest::skip(true)) {
  ::gandiva::FunctionRegistry registry;
  for (auto native_func_it = registry.begin(); native_func_it != registry.end();++native_func_it){
	SPDLOG_DEBUG("Function  |  pc_name: {}", native_func_it->pc_name());

	for (auto& sig : native_func_it->signatures()) {
	  auto sig_str = sig.ToString();
	  SPDLOG_DEBUG("          |  signature: {}", sig_str);
	}
  }
}

TEST_CASE ("cast-string-to-double" * doctest::skip(true)) {

  auto tuples = prepareTupleSet();

  SPDLOG_DEBUG("Input:\n{}", tuples->toString());

  auto expressions = std::vector<std::shared_ptr<normal::expression::gandiva::Expression>>{
	  cast(col("a"), float64Type()),
	  cast(col("b"), float64Type()),
	  cast(col("c"), float64Type())
  };

  auto projector = std::make_shared<Projector>(expressions);
  projector->compile(tuples->table()->schema());

  auto evaluated = tuples->evaluate(projector).value();
  SPDLOG_DEBUG("Output:\n{}", evaluated->toString());

  auto value_a_0 = evaluated->value<arrow::DoubleType>("a", 0).value();
	  CHECK_EQ(value_a_0, 1.0);
  auto value_b_1 = evaluated->value<arrow::DoubleType>("b", 1).value();
	  CHECK_EQ(value_b_1, 5.0);
  auto value_c_2 = evaluated->value<arrow::DoubleType>("c", 2).value();
	  CHECK_EQ(value_c_2, 9.0);
}