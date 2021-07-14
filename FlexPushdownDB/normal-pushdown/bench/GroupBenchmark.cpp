//
// Created by matt on 22/10/20.
//

#include <memory>

#include <doctest/doctest.h>

#include <nanobench.h>

#include <normal/tuple/Sample.h>
#include <normal/pushdown/group/GroupKernel.h>
#include <normal/pushdown/group/GroupKernel2.h>
#include <normal/pushdown/aggregate/Sum.h>
#include <normal/expression/gandiva/Column.h>
#include <normal/core/type/Float64Type.h>
#include <normal/expression/gandiva/Cast.h>
#include <normal/tuple/TupleSet2.h>

using namespace normal::tuple;
using namespace normal::pushdown::group;
using namespace normal::core::type;
using namespace normal::expression::gandiva;

#define SKIP_SUITE true

namespace {
void run(const std::shared_ptr<TupleSet2> &inputTupleSet) {

  std::shared_ptr<TupleSet2> outputTupleSet;

  ankerl::nanobench::Config().minEpochIterations(1).run(
	  fmt::format("group-{}-rows", inputTupleSet->numRows()), [&] {
		auto sumExpr = std::make_shared<Sum>("sum", cast(col("c_1"), float64Type()));
		std::vector<std::shared_ptr<AggregationFunction>> expressions{sumExpr};
		GroupKernel groupKernel({"c_0"}, expressions);
		groupKernel.onTuple(*inputTupleSet);
		outputTupleSet = groupKernel.group();
	  });

//  SPDLOG_DEBUG("Output:\n{}", outputTupleSet->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));

  ankerl::nanobench::Config().minEpochIterations(1).run(
	  fmt::format("group-{}-rows", inputTupleSet->numRows()), [&] {
		auto sumExpr = std::make_shared<Sum>("sum", cast(col("c_1"), float64Type()));
		std::vector<std::shared_ptr<AggregationFunction>> expressions{sumExpr};
		GroupKernel2 groupKernel({"c_0"}, {"c_1"}, expressions);
		groupKernel.group(*inputTupleSet);
		outputTupleSet = groupKernel.finalise().value();
	  });

//  SPDLOG_DEBUG("Output:\n{}", outputTupleSet->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));

}
}

TEST_SUITE ("group-benchmark" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("group-benchmark" * doctest::skip(false || SKIP_SUITE)) {

  auto tupleSet10 = Sample::sampleCxRIntString(10, 10);
  auto tupleSet100 = Sample::sampleCxRIntString(10, 100);
  auto tupleSet1000 = Sample::sampleCxRIntString(10, 1000);
  auto tupleSet10000 = Sample::sampleCxRIntString(10, 10000);
  auto tupleSet100000 = Sample::sampleCxRIntString(10, 100000);

//  run(tupleSet10);
//  run(tupleSet100);
//  run(tupleSet1000);
//  run(tupleSet10000);
  run(tupleSet100000);
}

}