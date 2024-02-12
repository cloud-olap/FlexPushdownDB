//
// Created by matt on 5/3/20.
//

#include <doctest/doctest.h>
#include <nanobench.h>
#include <memory>

#include <fpdb/executor/physical/shuffle/ShuffleKernel.h>
#include <fpdb/executor/physical/shuffle/ShuffleKernel2.h>
#include <fpdb/tuple/util/Sample.h>

using namespace fpdb::executor::physical::shuffle;
using namespace fpdb::tuple::util;

#define SKIP_SUITE false

void run(const std::shared_ptr<TupleSet> &tupleSet, const std::vector<std::string> &columnNames){
  const int numSlots = 32;
  std::vector<std::shared_ptr<TupleSet>> outputTupleSets;

  ankerl::nanobench::Config().minEpochIterations(1).run(
          fmt::format("[Original] shuffle-{}-rows", tupleSet->numRows()), [&] {
            outputTupleSets = ShuffleKernel::shuffle(columnNames, numSlots, *tupleSet).value();
          });

  for (int i = 0; i < numSlots; ++i){
    SPDLOG_DEBUG("Output {}:\n{}", i + 1, outputTupleSets[i]->showString(
            TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));
  }

  ankerl::nanobench::Config().minEpochIterations(1).run(
          fmt::format("[New] shuffle-{}-rows", tupleSet->numRows()), [&] {
            outputTupleSets = ShuffleKernel2::shuffle(columnNames, numSlots, tupleSet).value();
          });

  for (int i = 0; i < numSlots; ++i){
    SPDLOG_DEBUG("Output {}:\n{}", i + 1, outputTupleSets[i]->showString(
            TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));
  }
}

TEST_SUITE ("shuffle-benchmark" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("shuffle-benchmark-single-key" * doctest::skip(false || SKIP_SUITE)) {
  std::vector<std::string> columnNames{"c_0"};
  auto dist = std::uniform_int_distribution(0, 1000);   // shuffle test needs a wider range of values
  auto tupleSet10 = Sample::sampleCxRInt<long, arrow::Int64Type>(10, 10, dist);
  auto tupleSet100 = Sample::sampleCxRInt<long, arrow::Int64Type>(10, 100, dist);
  auto tupleSet1000 = Sample::sampleCxRInt<long, arrow::Int64Type>(10, 1000, dist);
  auto tupleSet10000 = Sample::sampleCxRInt<long, arrow::Int64Type>(10, 10000, dist);
  auto tupleSet100000 = Sample::sampleCxRInt<long, arrow::Int64Type>(10, 100000, dist);
  auto tupleSet500000 = Sample::sampleCxRInt<long, arrow::Int64Type>(10, 500000, dist);

  run(tupleSet10, columnNames);
  run(tupleSet100, columnNames);
  run(tupleSet1000, columnNames);
  run(tupleSet10000, columnNames);
  run(tupleSet100000, columnNames);
  run(tupleSet500000, columnNames);
}

TEST_CASE ("shuffle-benchmark-multi-key" * doctest::skip(false || SKIP_SUITE)) {
  std::vector<std::string> columnNames{"c_0", "c_1"};
    auto dist = std::uniform_int_distribution(0, 1000);   // shuffle test needs a wider range of values
  auto tupleSet10 = Sample::sampleCxRInt<long, arrow::Int64Type>(10, 10, dist);
  auto tupleSet100 = Sample::sampleCxRInt<long, arrow::Int64Type>(10, 100, dist);
  auto tupleSet1000 = Sample::sampleCxRInt<long, arrow::Int64Type>(10, 1000, dist);
  auto tupleSet10000 = Sample::sampleCxRInt<long, arrow::Int64Type>(10, 10000, dist);
  auto tupleSet100000 = Sample::sampleCxRInt<long, arrow::Int64Type>(10, 100000, dist);
  auto tupleSet500000 = Sample::sampleCxRInt<long, arrow::Int64Type>(10, 500000, dist);

  run(tupleSet10, columnNames);
  run(tupleSet100, columnNames);
  run(tupleSet1000, columnNames);
  run(tupleSet10000, columnNames);
  run(tupleSet100000, columnNames);
  run(tupleSet500000, columnNames);
}

}