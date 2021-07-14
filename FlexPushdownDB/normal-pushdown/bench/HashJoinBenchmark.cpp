//
// Created by matt on 31/7/20.
//

#include <memory>

#include <doctest/doctest.h>

#include <nanobench.h>

#include <normal/tuple/Sample.h>
#include <normal/pushdown/join/ATTIC/HashJoinBuildKernel.h>
#include <normal/pushdown/join/ATTIC/HashJoinProbeKernel.h>
#include <normal/pushdown/join/HashJoinProbeKernel2.h>
#include <normal/pushdown/join/HashJoinBuildKernel2.h>

using namespace normal::tuple;
using namespace normal::pushdown;
using namespace normal::pushdown::join;

#define SKIP_SUITE true

void runJoinBuild(const std::shared_ptr<TupleSet2> &tupleSet) {

  ankerl::nanobench::Config().minEpochIterations(1).run(
	  fmt::format("join-build-{}-rows", tupleSet->numRows()), [&] {
		auto buildKernel = HashJoinBuildKernel::make("c_0");
		buildKernel.put(tupleSet);
		auto hashTable = buildKernel.getHashTable();
	  });

  ankerl::nanobench::Config().minEpochIterations(1).run(
	  fmt::format("join-build2-{}-rows", tupleSet->numRows()), [&] {
		auto buildKernel = HashJoinBuildKernel2::make("c_0");
		auto result = buildKernel.put(tupleSet);
		if(!result.has_value()) throw std::runtime_error(result.error());
		auto arraySetIndex = buildKernel.getTupleSetIndex();
	  });

//  SPDLOG_DEBUG("Output:\n{}", hashTable.toString());

}

void run(const std::shared_ptr<TupleSet2> &tupleSet1, const std::shared_ptr<TupleSet2> &tupleSet2) {

  TupleSet2 joinedTupleSet;

  SPDLOG_DEBUG("BUILD RELATION:\n{}", tupleSet1->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));
  SPDLOG_DEBUG("PROBE RELATION:\n{}", tupleSet2->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));

  ankerl::nanobench::Config().minEpochIterations(1).run(
	  fmt::format("join-{}-{}-rows", tupleSet1->numRows(), tupleSet2->numRows()), [&] {
		auto buildKernel = HashJoinBuildKernel::make("c_0");
		buildKernel.put(tupleSet1);
		auto hashTable = buildKernel.getHashTable();

		auto probeKernel = HashJoinProbeKernel::make(JoinPredicate("c_0", "c_0"));
		probeKernel.putHashTable(hashTable);
		probeKernel.putTupleSet(tupleSet2);
		joinedTupleSet = *probeKernel.join().value();
	  });

  SPDLOG_DEBUG("Output:\n{}", joinedTupleSet.showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));

  ankerl::nanobench::Config().minEpochIterations(1).run(
	  fmt::format("join2-{}-{}-rows", tupleSet1->numRows(), tupleSet2->numRows()), [&] {

		tl::expected<void, std::string> result;

		auto buildKernel = HashJoinBuildKernel2::make("c_0");
		result = buildKernel.put(tupleSet1);
		if(!result.has_value()) throw std::runtime_error(result.error());
		auto index = buildKernel.getTupleSetIndex().value();
		auto indexTupleSet = TupleSet2::make(index->getTable());

//		SPDLOG_DEBUG("INDEX MAP:\n{}", index->toString());
//		SPDLOG_DEBUG("INDEX TUPLESET:\n{}", indexTupleSet->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));

    std::set<std::string> neededColumnNames;
    for (auto const &field: tupleSet1->schema().value()->fields()) {
      neededColumnNames.emplace(field->name());
    }
    for (auto const &field: tupleSet2->schema().value()->fields()) {
      neededColumnNames.emplace(field->name());
    }
		auto probeKernel = HashJoinProbeKernel2::make(JoinPredicate("c_0", "c_0"), neededColumnNames);
		result = probeKernel.joinBuildTupleSetIndex(index);
		if(!result.has_value()) throw std::runtime_error(result.error());
		result = probeKernel.joinProbeTupleSet(tupleSet2);
		if(!result.has_value()) throw std::runtime_error(result.error());
		joinedTupleSet = *probeKernel.getBuffer().value();
	  });

  SPDLOG_DEBUG("Output:\n{}", joinedTupleSet.showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));
}

TEST_SUITE ("join-benchmark" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("join-build-benchmark" * doctest::skip(false || SKIP_SUITE)) {

  auto tupleSet10 = Sample::sampleCxRIntString(10, 10);
  auto tupleSet100 = Sample::sampleCxRIntString(10, 100);
  auto tupleSet1000 = Sample::sampleCxRIntString(10, 1000);
  auto tupleSet10000 = Sample::sampleCxRIntString(10, 10000);
  auto tupleSet100000 = Sample::sampleCxRIntString(10, 100000);

  runJoinBuild(tupleSet10);
  runJoinBuild(tupleSet100);
  runJoinBuild(tupleSet1000);
  runJoinBuild(tupleSet10000);
  runJoinBuild(tupleSet100000);
}

TEST_CASE ("join-build-probe-benchmark" * doctest::skip(false || SKIP_SUITE)) {

  auto tupleSet10 = Sample::sampleCxRIntString(10, 10);
  auto tupleSet100 = Sample::sampleCxRIntString(10, 100);
  auto tupleSet1000 = Sample::sampleCxRIntString(10, 1000);
  auto tupleSet10000 = Sample::sampleCxRIntString(10, 10000);
//  auto tupleSet100000 = Sample::sampleCxRIntString(10, 100000);

//  run(tupleSet10, tupleSet100);
  run(tupleSet100, tupleSet1000);
//  run(tupleSet1000, tupleSet10000);
//  run(tupleSet10000, tupleSet100000);
}

}