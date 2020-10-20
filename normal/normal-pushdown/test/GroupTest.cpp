//
// Created by matt on 13/5/20.
//

#include <memory>
#include <filesystem>

#include <doctest/doctest.h>

#include <normal/core/Normal.h>
#include <normal/pushdown/file/FileScan.h>
#include <normal/pushdown/group/Group.h>
#include <normal/pushdown/aggregate/Sum.h>
#include <normal/expression/gandiva/Column.h>
#include <normal/core/type/Float64Type.h>
#include <normal/expression/gandiva/Cast.h>
#include "TestUtil.h"

using namespace std::filesystem;
using namespace normal::pushdown;
using namespace normal::pushdown::test;
using namespace normal::pushdown::group;
using namespace normal::core::type;
using namespace normal::expression::gandiva;

TEST_SUITE ("group") {

TEST_CASE ("group" * doctest::skip(false)) {
  {
	auto n = Normal::start();
	auto g = n->createQuery();

	auto aFile = filesystem::absolute("data/group/a.csv");
	auto numBytesAFile = filesystem::file_size(aFile);

	auto scan = FileScan::make("fileScan",
							   "data/group/a.csv",
							   std::vector<std::string>{"AA", "AB"},
							   0,
							   numBytesAFile,
							   g->getId(),
							   true);
	g->put(scan);
	auto sumExpr = std::make_shared<normal::pushdown::aggregate::Sum>("sum",
																	  cast(col("AB"), float64Type()));
	auto
		expressions2 =
		std::make_shared<std::vector<std::shared_ptr<normal::pushdown::aggregate::AggregationFunction>>>();
	expressions2->push_back(sumExpr);

	auto group = Group::make("group", {"AA"}, expressions2);
	g->put(group);
	auto collate = std::make_shared<Collate>("collate", g->getId());
	g->put(collate);

	scan->produce(group);
	group->consume(scan);

	group->produce(collate);
	collate->consume(group);

	TestUtil::writeExecutionPlan(*g);

	auto expectedTupleSet = g->execute();
		REQUIRE(expectedTupleSet.has_value());
	auto tupleSet = expectedTupleSet.value();

	SPDLOG_INFO("Output:\n{}", tupleSet->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));

		REQUIRE_EQ(tupleSet->numRows(), 2);
		REQUIRE_EQ(tupleSet->numColumns(), 2);

	/*
	 * FIXME: The following assumes the output is produced in a specific order but this shouldn't necessarily
	 *  be assumed. Will only be able to check properly once we have a sort operator
	 */
	auto columnAA = tupleSet->getColumnByName("AA").value();
		REQUIRE_EQ(columnAA->element(0).value()->value<std::string>(), "10");
		REQUIRE_EQ(columnAA->element(1).value()->value<std::string>(), "12");

	auto columnAB = tupleSet->getColumnByName("sum").value();
		REQUIRE_EQ(columnAB->element(0).value()->value<double>(), 27);
		REQUIRE_EQ(columnAB->element(1).value()->value<double>(), 15);
	n->stop();
  }
	  REQUIRE_EQ(::arrow::default_memory_pool()->bytes_allocated(), 0);
}

}