//
// Created by matt on 31/7/20.
//

#include <doctest/doctest.h>
#include <nanobench.h>
#include <memory>

#include <fpdb/executor/physical/join/hashjoin/HashJoinBuildKernel.h>
#include <fpdb/executor/physical/join/hashjoin/HashJoinProbeKernel.h>
#include <fpdb/executor/physical/join/hashjoin/HashJoinArrowKernel.h>
#include <fpdb/tuple/util/Sample.h>

using namespace fpdb::executor::physical::join;
using namespace fpdb::tuple::util;

#define SKIP_SUITE false

void run(const std::shared_ptr<TupleSet> &leftTupleSet, const std::shared_ptr<TupleSet> &rightTupleSet,
         const std::vector<std::string> &leftColumnNames, const std::vector<std::string> &rightColumnNames,
         const std::set<std::string> &neededColumnNames) {
  std::shared_ptr<TupleSet> outputTupleSet;

  // original kernel
  ankerl::nanobench::Config().minEpochIterations(1).run(
          fmt::format("[Original] hashjoin-{}-{}-rows", leftTupleSet->numRows(), rightTupleSet->numRows()), [&] {

            // build
            auto buildKernel = HashJoinBuildKernel::make(leftColumnNames);
            auto result = buildKernel.put(leftTupleSet);
            if (!result.has_value()) {
              throw std::runtime_error(result.error());
            }
            auto expTupleSetIndex = buildKernel.getTupleSetIndex();
            if (!expTupleSetIndex.has_value()) {
              throw std::runtime_error("TupleSetIndex not built yet");
            }

            // probe
            auto probeKernel = HashJoinProbeKernel::make(HashJoinPredicate(leftColumnNames, rightColumnNames),
                                                         neededColumnNames, false, false);
            result = probeKernel->joinBuildTupleSetIndex(*expTupleSetIndex);
            if (!result.has_value()) {
              throw std::runtime_error(result.error());
            }
            result = probeKernel->joinProbeTupleSet(rightTupleSet);
            if (!result.has_value()) {
              throw std::runtime_error(result.error());
            }

            // output
            auto expOutputTupleSet = probeKernel->getBuffer();
            if (!expOutputTupleSet.has_value()) {
              throw std::runtime_error("No hash join result buffered yet");
            }
            outputTupleSet = *expOutputTupleSet;

          });

  SPDLOG_DEBUG("Output:\n{}", outputTupleSet->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));

  // arrow kernel
  ankerl::nanobench::Config().minEpochIterations(1).run(
          fmt::format("[Arrow] hashjoin-{}-{}-rows", leftTupleSet->numRows(), rightTupleSet->numRows()), [&] {

            auto kernel = HashJoinArrowKernel::make(HashJoinPredicate(leftColumnNames, rightColumnNames),
                                                    neededColumnNames, JoinType::INNER);

            // build
            auto res = kernel.joinBuildTupleSet(leftTupleSet);
            if (!res.has_value()) {
              throw std::runtime_error(res.error());
            }
            kernel.finalizeInput(true);

            // probe
            res = kernel.joinProbeTupleSet(rightTupleSet);
            if (!res.has_value()) {
              throw std::runtime_error(res.error());
            }
            kernel.finalizeInput(false);

            // output
            auto expOutputTupleSet = kernel.getOutputBuffer();
            if (!expOutputTupleSet.has_value()) {
              throw std::runtime_error("No hash join result buffered yet");
            }
            outputTupleSet = *expOutputTupleSet;

          });

  SPDLOG_DEBUG("Output:\n{}", outputTupleSet->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));
}

namespace fpdb::executor::bench {

TEST_SUITE ("hashjoin-benchmark" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("hashjoin-benchmark-single-join-key" * doctest::skip(false || SKIP_SUITE)) {
  std::vector<std::string> leftColumnNames{"c_0"};
  std::vector<std::string> rightColumnNames{"c_0"};
  std::set<std::string> neededColumnNames{"c_0", "c_1", "c_2"};
  auto tupleSet10 = Sample::sampleCxRInt<long, arrow::Int64Type>(10, 10);
  auto tupleSet100 = Sample::sampleCxRInt<long, arrow::Int64Type>(10, 100);
  auto tupleSet1000 = Sample::sampleCxRInt<long, arrow::Int64Type>(10, 1000);
  auto tupleSet10000 = Sample::sampleCxRInt<long, arrow::Int64Type>(10, 10000);

  run(tupleSet10, tupleSet100, leftColumnNames, rightColumnNames, neededColumnNames);
  run(tupleSet10, tupleSet1000, leftColumnNames, rightColumnNames, neededColumnNames);
  run(tupleSet10, tupleSet10000, leftColumnNames, rightColumnNames, neededColumnNames);
  run(tupleSet100, tupleSet1000, leftColumnNames, rightColumnNames, neededColumnNames);
  run(tupleSet100, tupleSet10000, leftColumnNames, rightColumnNames, neededColumnNames);
  run(tupleSet1000, tupleSet10000, leftColumnNames, rightColumnNames, neededColumnNames);
}

TEST_CASE ("hashjoin-benchmark-multi-join-key" * doctest::skip(false || SKIP_SUITE)) {
  std::vector<std::string> leftColumnNames{"c_0", "c_1"};
  std::vector<std::string> rightColumnNames{"c_0", "c_1"};
  std::set<std::string> neededColumnNames{"c_0", "c_1", "c_2"};
  auto tupleSet10 = Sample::sampleCxRInt<long, arrow::Int64Type>(10, 10);
  auto tupleSet100 = Sample::sampleCxRInt<long, arrow::Int64Type>(10, 100);
  auto tupleSet1000 = Sample::sampleCxRInt<long, arrow::Int64Type>(10, 1000);
  auto tupleSet10000 = Sample::sampleCxRInt<long, arrow::Int64Type>(10, 10000);

  run(tupleSet10, tupleSet100, leftColumnNames, rightColumnNames, neededColumnNames);
  run(tupleSet10, tupleSet1000, leftColumnNames, rightColumnNames, neededColumnNames);
  run(tupleSet10, tupleSet10000, leftColumnNames, rightColumnNames, neededColumnNames);
  run(tupleSet100, tupleSet1000, leftColumnNames, rightColumnNames, neededColumnNames);
  run(tupleSet100, tupleSet10000, leftColumnNames, rightColumnNames, neededColumnNames);
  run(tupleSet1000, tupleSet10000, leftColumnNames, rightColumnNames, neededColumnNames);
}

}

}