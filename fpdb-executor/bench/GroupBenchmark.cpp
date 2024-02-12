//
// Created by matt on 22/10/20.
//

#include <doctest/doctest.h>
#include <nanobench.h>
#include <memory>

#include <fpdb/executor/physical/group/GroupKernel.h>
#include <fpdb/executor/physical/group/GroupArrowKernel.h>
#include <fpdb/executor/physical/aggregate/function/Sum.h>
#include <fpdb/expression/gandiva/Cast.h>
#include <fpdb/expression/gandiva/Column.h>
#include <fpdb/tuple/TupleSet.h>
#include <fpdb/tuple/util/Sample.h>

using namespace fpdb::executor::physical::group;
using namespace fpdb::executor::physical::aggregate;
using namespace fpdb::expression::gandiva;
using namespace fpdb::tuple::util;

#define SKIP_SUITE false

void run(const std::shared_ptr<TupleSet> &inputTupleSet) {
  std::shared_ptr<TupleSet> outputTupleSet;

  // group kernel
  ankerl::nanobench::Config().minEpochIterations(1).run(
          fmt::format("[GroupKernel] group-{}-rows", inputTupleSet->numRows()), [&] {

            std::vector<std::shared_ptr<AggregateFunction>> aggFunctions{
                    std::make_shared<Sum>("sum", col("c_2"))
            };
            GroupKernel groupKernel({"c_0", "c_1"}, aggFunctions);

            auto groupRes = groupKernel.group(inputTupleSet);
            if (!groupRes.has_value()) {
              throw std::runtime_error(groupRes.error());
            }

            auto expOutputTupleSet = groupKernel.finalise();
            if (!expOutputTupleSet.has_value()) {
              throw std::runtime_error(expOutputTupleSet.error());
            }
            outputTupleSet = *expOutputTupleSet;

          });

  SPDLOG_DEBUG("Output:\n{}", outputTupleSet->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));

  // group arrow kernel
  ankerl::nanobench::Config().minEpochIterations(1).run(
          fmt::format("[GroupArrowKernel] group-{}-rows", inputTupleSet->numRows()), [&] {

            std::vector<std::shared_ptr<AggregateFunction>> aggFunctions{
                    std::make_shared<Sum>("sum", col("c_2"))
            };
            GroupArrowKernel groupKernel({"c_0", "c_1"}, aggFunctions);

            auto groupRes = groupKernel.group(inputTupleSet);
            if (!groupRes.has_value()) {
              throw std::runtime_error(groupRes.error());
            }

            auto expOutputTupleSet = groupKernel.finalise();
            if (!expOutputTupleSet.has_value()) {
              throw std::runtime_error(expOutputTupleSet.error());
            }
            outputTupleSet = *expOutputTupleSet;

          });

  SPDLOG_DEBUG("Output:\n{}", outputTupleSet->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));
}

namespace fpdb::executor::bench {

TEST_SUITE ("group-benchmark" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("group-benchmark-dense-group" * doctest::skip(false || SKIP_SUITE)) {
  auto tupleSet10 = Sample::sampleCxRInt<long, arrow::Int64Type>(10, 10, std::uniform_int_distribution(0, 1));          // ~4 groups
  auto tupleSet100 = Sample::sampleCxRInt<long, arrow::Int64Type>(10, 100, std::uniform_int_distribution(0, 2));        // ~9 groups
  auto tupleSet1000 = Sample::sampleCxRInt<long, arrow::Int64Type>(10, 1000, std::uniform_int_distribution(0, 3));      // ~16 groups
  auto tupleSet10000 = Sample::sampleCxRInt<long, arrow::Int64Type>(10, 10000, std::uniform_int_distribution(0, 4));    // ~25 groups
  auto tupleSet100000 = Sample::sampleCxRInt<long, arrow::Int64Type>(10, 100000, std::uniform_int_distribution(0, 5));  // ~36 groups

  run(tupleSet10);
  run(tupleSet100);
  run(tupleSet1000);
  run(tupleSet10000);
  run(tupleSet100000);
}

TEST_CASE ("group-benchmark-sparse-group" * doctest::skip(false || SKIP_SUITE)) {
  auto tupleSet10 = Sample::sampleCxRInt<long, arrow::Int64Type>(10, 10, std::uniform_int_distribution(0, 2));           // ~9 groups
  auto tupleSet100 = Sample::sampleCxRInt<long, arrow::Int64Type>(10, 100, std::uniform_int_distribution(0, 3));         // ~16 groups
  auto tupleSet1000 = Sample::sampleCxRInt<long, arrow::Int64Type>(10, 1000, std::uniform_int_distribution(0, 9));       // ~100 groups
  auto tupleSet10000 = Sample::sampleCxRInt<long, arrow::Int64Type>(10, 10000, std::uniform_int_distribution(0, 29));    // ~900 groups
  auto tupleSet100000 = Sample::sampleCxRInt<long, arrow::Int64Type>(10, 100000, std::uniform_int_distribution(0, 100)); // ~10000 groups

  run(tupleSet10);
  run(tupleSet100);
  run(tupleSet1000);
  run(tupleSet10000);
  run(tupleSet100000);
}

}

}