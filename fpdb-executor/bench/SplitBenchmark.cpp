//
// Created by Yifei Yang on 2/5/24.
//

#include <doctest/doctest.h>
#include <nanobench.h>
#include <memory>

#include <fpdb/executor/physical/split/SplitKernel.h>
#include <fpdb/tuple/util/Sample.h>

using namespace fpdb::executor::physical::split;
using namespace fpdb::tuple::util;
using namespace fpdb::tuple;

#define SKIP_SUITE false

TEST_SUITE ("split-benchmark" * doctest::skip(SKIP_SUITE)) {

std::shared_ptr<TupleSet> makeTestTable(int numRows, int numBatches) {
  int rowsPerBatch = numRows / numBatches;
  std::vector<std::shared_ptr<TupleSet>> singleBatchTupleSets;
  for (int i = 0; i < numBatches; ++i) {
    singleBatchTupleSets.emplace_back(
            Sample::sampleCxRInt<long, arrow::Int64Type>(10, rowsPerBatch, std::uniform_int_distribution(0, numRows)));
  }
  auto expTestTupleSet = TupleSet::concatenate(singleBatchTupleSets);
  if (!expTestTupleSet.has_value()) {
    throw std::runtime_error(expTestTupleSet.error());
  }
  return *expTestTupleSet;
}

void runOne(const std::shared_ptr<TupleSet> &tupleSet, int n) {
  ankerl::nanobench::Config().minEpochIterations(1).run(
          fmt::format("[Original] split-{}-rows", tupleSet->numRows()), [&] {
            SplitKernel::split(tupleSet, n);
          });

  ankerl::nanobench::Config().minEpochIterations(1).run(
          fmt::format("[New] split-{}-rows", tupleSet->numRows()), [&] {
            SplitKernel::split2(tupleSet, n);
          });
}

void run(uint n, int numBatches) {
  auto tupleSet1000 = makeTestTable(1000, numBatches);
  auto tupleSet10000 = makeTestTable(10000, numBatches);
  auto tupleSet100000 = makeTestTable(100000, numBatches);
  auto tupleSet1000000 = makeTestTable(1000000, numBatches);
  auto tupleSet10000000 = makeTestTable(10000000, numBatches);

  printf("[n = %u, numBatches = %d]:\n", n, numBatches);
  runOne(tupleSet1000, n);
  runOne(tupleSet10000, n);
  runOne(tupleSet100000, n);
  runOne(tupleSet1000000, n);
  runOne(tupleSet10000000, n);
  printf("\n");
}

TEST_CASE ("split-one-batch" * doctest::skip(false || SKIP_SUITE)) {
  run(16, 1);
}

TEST_CASE ("split-few-batches" * doctest::skip(false || SKIP_SUITE)) {
  run(16, 20);
}

TEST_CASE ("split-many-batches" * doctest::skip(false || SKIP_SUITE)) {
  run(16, 1000);
}

}
