//
// Created by matt on 27/7/20.
//
#include <memory>
#include <vector>

#include <doctest/doctest.h>

#include <gandiva/tree_expr_builder.h>
#include <normal/expression/gandiva/Projector.h>
#include <normal/expression/gandiva/Column.h>
#include <normal/core/type/Float64Type.h>
#include <normal/core/type/Integer32Type.h>
#include <normal/expression/gandiva/Cast.h>
#include <normal/expression/gandiva/Multiply.h>
#include <normal/expression/gandiva/Add.h>
#include <normal/tuple/TupleSet2.h>
#include <random>
#include <normal/tuple/Sample.h>

#include "Globals.h"
#include "TestUtil.h"

using namespace normal::core::type;
using namespace normal::tuple;
using namespace normal::expression::gandiva;
using namespace normal::expression::gandiva::test;

#define SKIP_SUITE false

TEST_SUITE ("thread" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("make" * doctest::skip(false || SKIP_SUITE)) {

  std::vector<std::thread> workers;
  workers.reserve(50);
  for (int i = 0; i < 50; i++) {
	workers.emplace_back([]() {

	  SPDLOG_DEBUG("Start");

	  auto e = times(cast(col("c_0"), float64Type()),
					 cast(col("c_1"), float64Type()));

	  auto tupleSet = Sample::sampleCxRString(2, 1000000);

	  SPDLOG_DEBUG("Input:\n{}", tupleSet->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));

	  Projector p({e});
	  p.compile(tupleSet->schema().value()->getSchema());

	  auto results1 = p.evaluate(*tupleSet->toTupleSetV1());
	  SPDLOG_DEBUG("Output 1:\n{}", TupleSet2::create(results1)->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));

	  auto results2 = p.evaluate(*tupleSet->toTupleSetV1());
	  SPDLOG_DEBUG("Output 2:\n{}", TupleSet2::create(results2)->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));

	  SPDLOG_DEBUG("Finish");
	});
  }

  SPDLOG_DEBUG("Joining");
  std::for_each(workers.begin(), workers.end(), [](std::thread &t) {
	t.join();
  });

}

}
