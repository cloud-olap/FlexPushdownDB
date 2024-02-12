//
// Created by matt on 27/7/20.
//
#include <vector>

#include <doctest/doctest.h>

#include <gandiva/tree_expr_builder.h>
#include <fpdb/expression/gandiva/Projector.h>
#include <fpdb/expression/gandiva/Column.h>
#include <fpdb/expression/gandiva/Cast.h>
#include <fpdb/expression/gandiva/Multiply.h>
#include <fpdb/tuple/TupleSet.h>
#include <fpdb/tuple/Sample.h>

#include "Globals.h"

using namespace fpdb::tuple;
using namespace fpdb::expression::gandiva;
using namespace fpdb::expression::gandiva::test;

#define SKIP_SUITE true

TEST_SUITE ("thread" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("make" * doctest::skip(false || SKIP_SUITE)) {

  std::vector<std::thread> workers;
  workers.reserve(50);
  for (int i = 0; i < 50; i++) {
	workers.emplace_back([]() {

	  SPDLOG_DEBUG("Start");

	  auto e = times(cast(col("c_0"), arrow::float64()),
					 cast(col("c_1"), arrow::float64()));

	  auto tupleSet = Sample::sampleCxRString(2, 1000000);

	  SPDLOG_DEBUG("Input:\n{}", tupleSet->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));

	  Projector p({e});
	  p.compile(tupleSet->schema());

	  auto results1 = *p.evaluate(*tupleSet);
	  SPDLOG_DEBUG("Output 1:\n{}", results1->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));

	  auto results2 = *p.evaluate(*tupleSet);
	  SPDLOG_DEBUG("Output 2:\n{}", results2->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));

	  SPDLOG_DEBUG("Finish");
	});
  }

  SPDLOG_DEBUG("Joining");
  std::for_each(workers.begin(), workers.end(), [](std::thread &t) {
	t.join();
  });

}

}
