//
// Created by matt on 5/3/20.
//

#include <memory>

#include <doctest/doctest.h>

#include <nanobench.h>

#include <normal/pushdown/shuffle/ATTIC/Shuffler.h>
#include <normal/pushdown/shuffle/ATTIC/ShuffleKernel.h>
#include <normal/tuple/Sample.h>
#include <normal/pushdown/shuffle/ShuffleKernel2.h>

using namespace normal::tuple;
using namespace normal::pushdown;
using namespace normal::pushdown::shuffle;

#define SKIP_SUITE false

void run(const std::shared_ptr<TupleSet2> &tupleSet){

  std::vector<std::shared_ptr<TupleSet2>> shuffledTupleSets;

  ankerl::nanobench::Config().minEpochIterations(1).run(
	  fmt::format("shuffle-{}-rows", tupleSet->numRows()), [&] {
		shuffledTupleSets = Shuffler::shuffle("c_0", 2, tupleSet).value();
	  });

  //  for(const auto &shuffledTupleSet: shuffledTupleSets){
//	SPDLOG_DEBUG("Output:\n{}", shuffledTupleSet->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));
//  }

  ankerl::nanobench::Config().minEpochIterations(1).run(
	  fmt::format("shuffle2-{}-rows", tupleSet->numRows()), [&] {
		shuffledTupleSets = ShuffleKernel::shuffle("c_0", 2, tupleSet).value();
	  });

//  for(const auto &shuffledTupleSet: shuffledTupleSets){
//	SPDLOG_DEBUG("Output:\n{}", shuffledTupleSet->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));
//  }

  ankerl::nanobench::Config().minEpochIterations(1).run(
	  fmt::format("shuffle3-{}-rows", tupleSet->numRows()), [&] {
		shuffledTupleSets = ShuffleKernel2::shuffle("c_0", 2, *tupleSet).value();
	  });

//  for(const auto &shuffledTupleSet: shuffledTupleSets){
//	SPDLOG_DEBUG("Output:\n{}", shuffledTupleSet->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));
//  }
}

TEST_SUITE ("shuffle-benchmark" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("scaling" * doctest::skip(false || SKIP_SUITE)) {

  auto tupleSet100 = Sample::sampleCxRString(10, 100);
  auto tupleSet1000 = Sample::sampleCxRString(10, 1000);
  auto tupleSet10000 = Sample::sampleCxRString(10, 10000);
  auto tupleSet100000 = Sample::sampleCxRString(10, 100000);


  run(tupleSet100);
  run(tupleSet1000);
  run(tupleSet10000);
  run(tupleSet100000);
}

}