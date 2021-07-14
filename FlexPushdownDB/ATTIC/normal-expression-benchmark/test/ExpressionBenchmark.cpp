//
// Created by matt on 24/4/20.
//

#include <vector>

#include <doctest/doctest.h>
#include <nanobench.h>

#include <normal/tuple/arrow/Arrays.h>
#include <normal/tuple/TupleSet.h>
#include <normal/core/type/DecimalType.h>
#include <normal/core/type/Float64Type.h>

#include <normal/expression/simple/Expression.h>
#include <normal/expression/simple/Column.h>
#include <normal/expression/simple/Cast.h>
#include <normal/expression/simple/Projector.h>

#include <normal/expression/gandiva/Expression.h>
#include <normal/expression/gandiva/Column.h>
#include <normal/expression/gandiva/Cast.h>
#include <normal/expression/gandiva/Projector.h>

using namespace normal::core::type;

namespace spl = normal::expression::simple;
namespace gdv = normal::expression::gandiva;

std::shared_ptr<TupleSet> prepareRandomTupleSet(unsigned long numColumns, unsigned long numRows) {

  // Create fields
  std::vector<std::shared_ptr<arrow::Field>> fields;
  fields.reserve(numColumns);
  for (unsigned long c = 0; c < numColumns; c++) {
	fields.emplace_back(arrow::field(std::to_string(c), arrow::utf8()));
  }
  auto schema = arrow::schema(fields);

  // Create data
  int counter = 0;
  std::vector<std::shared_ptr<arrow::Array>> arrowArrays;
  for (unsigned long c = 0; c < numColumns; c++) {
	std::vector<std::string> column;
	column.reserve(numRows);
	for (unsigned long r = 0; r < numRows; r++) {
	  column.emplace_back(std::to_string(counter++));
	}
	auto arrowArray = Arrays::make<arrow::StringType>(column);
	arrowArrays.emplace_back(arrowArray.value());
  }

  auto tuples = TupleSet::make(schema, arrowArrays);

  return tuples;
}

TEST_CASE ("benchmark-expression") {

  auto tuples = prepareRandomTupleSet(3, 10000);

  SPDLOG_DEBUG("Input:\n{}", tuples->toString());

  auto splExpressions = std::vector<std::shared_ptr<normal::expression::simple::Expression>>{
	  spl::cast(spl::col("0"), float64Type()),
	  spl::cast(spl::col("1"), float64Type()),
	  spl::cast(spl::col("2"), float64Type())
  };

  auto simpleProjector = std::make_shared<normal::expression::simple::Projector>(splExpressions);

  auto gdvExpressions = std::vector<std::shared_ptr<normal::expression::gandiva::Expression>>{
	  gdv::cast(gdv::col("0"), decimalType(10, 5)),
	  gdv::cast(gdv::col("1"), decimalType(10, 5)),
	  gdv::cast(gdv::col("2"), decimalType(10, 5))
  };

  auto gandivaProjector = std::make_shared<normal::expression::gandiva::Projector>(gdvExpressions);
  gandivaProjector->compile(tuples->table()->schema());

  ankerl::nanobench::Config().minEpochIterations(10).run(
	  "evaluate-simple-cast-string-to-decimal-reuse-projector", [&] {
		auto evaluated = simpleProjector->evaluate(*tuples);
	  });

  ankerl::nanobench::Config().minEpochIterations(10).run(
	  "evaluate-gandiva-cast-string-to-decimal-reuse-projector",
	  [&] {
		auto evaluated = gandivaProjector->evaluate(*tuples);
	  });
}
