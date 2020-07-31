//
// Created by matt on 31/7/20.
//

#include <memory>

#include <doctest/doctest.h>

#include <nanobench.h>

#include <normal/tuple/Sample.h>
#include <normal/pushdown/join/Joiner.h>

using namespace normal::tuple;
using namespace normal::pushdown;
using namespace normal::pushdown::join;

#define SKIP_SUITE false

void run(const TupleSet2 &tupleSet1, const TupleSet2 &tupleSet2){

  TupleSet2 joinedTupleSet;

//  ankerl::nanobench::Config().minEpochIterations(1).run(
//	  fmt::format("join-{}-rows", tupleSet1.numRows()), [&] {
//		joinedTupleSet = Joiner::join("c_0", tupleSet1, tupleSet2).value();
//	  });

  //  for(const auto &shuffledTupleSet: shuffledTupleSets){
//	SPDLOG_DEBUG("Output:\n{}", shuffledTupleSet->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));
//  }
}

TEST_SUITE ("shuffle-benchmark" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("scaling" * doctest::skip(false || SKIP_SUITE)) {

  auto tupleSet100_1 = Sample::sampleCxRIntString(10, 100);
  auto tupleSet100_2 = Sample::sampleCxRIntString(10, 100);
 auto tupleSet1000_1 = Sample::sampleCxRIntString(10, 1000);
  auto tupleSet1000_2= Sample::sampleCxRIntString(10, 1000);
  auto tupleSet10000_1 = Sample::sampleCxRIntString(10, 10000);
  auto tupleSet10000_2 = Sample::sampleCxRIntString(10, 10000);
  auto tupleSet100000_1 = Sample::sampleCxRIntString(10, 100000);
  auto tupleSet100000_2 = Sample::sampleCxRIntString(10, 100000);

  run(*tupleSet100_1, *tupleSet100_2);
  run(*tupleSet1000_1, *tupleSet1000_2);
  run(*tupleSet10000_1, *tupleSet10000_2);
  run(*tupleSet100000_1, *tupleSet100000_2);
}

}