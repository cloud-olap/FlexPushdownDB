//
// Created by matt on 24/4/20.
//

#include <vector>

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

std::shared_ptr<normal::core::TupleSet> prepareRandomTupleSet(int numColumns, int numRows) {

  // Create fields
  std::vector<std::shared_ptr<arrow::Field>> fields;
  fields.reserve(numColumns);
	for(int c=0;c<numColumns;c++){
	fields.emplace_back(arrow::field(std::to_string(c), arrow::utf8()));
  }
  auto schema = arrow::schema(fields);

  // Create data
  int counter = 0;
  std::vector<std::shared_ptr<arrow::Array>> arrowArrays;
  for(int c=0;c<numColumns;c++){
	std::vector<std::string> column;
	column.reserve(numRows);
	for(int r=0;r<numRows;r++){
	  column.emplace_back(std::to_string(counter++));
	}
	auto arrowArray = Arrays::make<arrow::StringType>(column);
	arrowArrays.emplace_back(arrowArray.value());
  }

  auto tuples = normal::core::TupleSet::make(schema, arrowArrays);

  return tuples;
}

TEST_CASE ("benchmark-expression") {

  auto tuples = prepareRandomTupleSet(3, 10000);

//  SPDLOG_DEBUG("Input:\n{}", tuples->toString());

  auto expressions = std::vector<std::shared_ptr<normal::expression::Expression>>{
	  cast(col("0"), decimalType(10, 5)),
	  cast(col("1"), decimalType(10, 5)),
	  cast(col("2"), decimalType(10, 5))
  };

  auto projector = std::make_shared<Projector>(expressions);
  projector->compile(tuples->table()->schema());

//  ankerl::nanobench::Config().minEpochIterations(10).run("evaluate-cast-string-to-decimal", [&] {
//	auto projector = std::make_shared<Projector>(expressions);
//	projector->compile(tuples->table()->schema());
//
//	auto evaluated = tuples->evaluate(projector).value();
//  });

  ankerl::nanobench::Config().minEpochIterations(10).run("evaluate-cast-string-to-decimal-reuse-projector", [&] {
	auto evaluated = tuples->evaluate(projector).value();
  });
}
