//
// Created by matt on 13/5/20.
//

#include <memory>
#include <filesystem>

#include <doctest/doctest.h>

#include <normal/core/ATTIC/Normal.h>
#include <normal/pushdown/file/FileScan.h>
#include <normal/pushdown/group/Group.h>
#include <normal/pushdown/group/GroupKernel.h>
#include <normal/pushdown/group/GroupKernel2.h>
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

namespace {

auto makeUndefinedTupleSet() {
  auto tupleSet = TupleSet2::make();
  return tupleSet;
}

auto makeEmptyTupleSet() {
  auto schemaA = ::arrow::schema({::arrow::field("aa", ::arrow::int64()),
								  ::arrow::field("ab", ::arrow::int64()),
								  ::arrow::field("ac", ::arrow::int64())});

  auto arrayAA1 = Arrays::make<arrow::Int64Type>({}).value();
  auto arrayAA = std::make_shared<arrow::ChunkedArray>(arrow::ArrayVector{arrayAA1});
  auto arrayAB1 = Arrays::make<arrow::Int64Type>({}).value();
  auto arrayAB = std::make_shared<arrow::ChunkedArray>(arrow::ArrayVector{arrayAB1});
  auto arrayAC1 = Arrays::make<arrow::Int64Type>({}).value();
  auto arrayAC = std::make_shared<arrow::ChunkedArray>(arrow::ArrayVector{arrayAC1});
  auto tableA = arrow::Table::Make(schemaA, {arrayAA, arrayAB, arrayAC});
  auto tupleSetA = TupleSet2::make(tableA);
  return tupleSetA;
}

auto makeTupleSet1() {
  auto schemaA = ::arrow::schema({::arrow::field("aa", ::arrow::int64()),
								  ::arrow::field("ab", ::arrow::int64()),
								  ::arrow::field("ac", ::arrow::int64())});

  auto arrayAA1 = Arrays::make<arrow::Int64Type>({1, 2}).value();
  auto arrayAA2 = Arrays::make<arrow::Int64Type>({3}).value();
  auto arrayAA = std::make_shared<arrow::ChunkedArray>(arrow::ArrayVector{arrayAA1, arrayAA2});
  auto arrayAB1 = Arrays::make<arrow::Int64Type>({4, 5}).value();
  auto arrayAB2 = Arrays::make<arrow::Int64Type>({6}).value();
  auto arrayAB = std::make_shared<arrow::ChunkedArray>(arrow::ArrayVector{arrayAB1, arrayAB2});
  auto arrayAC1 = Arrays::make<arrow::Int64Type>({7, 8}).value();
  auto arrayAC2 = Arrays::make<arrow::Int64Type>({9}).value();
  auto arrayAC = std::make_shared<arrow::ChunkedArray>(arrow::ArrayVector{arrayAC1, arrayAC2});
  auto tableA = arrow::Table::Make(schemaA, {arrayAA, arrayAB, arrayAC});
  auto tupleSetA = TupleSet2::make(tableA);
  return tupleSetA;
}

auto makeTupleSet2() {
  auto schemaB = ::arrow::schema({::arrow::field("aa", ::arrow::int64()),
								  ::arrow::field("ab", ::arrow::int64()),
								  ::arrow::field("ac", ::arrow::int64())});

  auto arrayBA1 = Arrays::make<arrow::Int64Type>({1, 2}).value();
  auto arrayBA2 = Arrays::make<arrow::Int64Type>({3}).value();
  auto arrayBA = std::make_shared<arrow::ChunkedArray>(arrow::ArrayVector{arrayBA1, arrayBA2});
  auto arrayBB1 = Arrays::make<arrow::Int64Type>({14, 15}).value();
  auto arrayBB2 = Arrays::make<arrow::Int64Type>({16}).value();
  auto arrayBB = std::make_shared<arrow::ChunkedArray>(arrow::ArrayVector{arrayBB1, arrayBB2});
  auto arrayBC1 = Arrays::make<arrow::Int64Type>({17, 18}).value();
  auto arrayBC2 = Arrays::make<arrow::Int64Type>({19}).value();
  auto arrayBC = std::make_shared<arrow::ChunkedArray>(arrow::ArrayVector{arrayBC1, arrayBC2});
  auto tableB = arrow::Table::Make(schemaB, {arrayBA, arrayBB, arrayBC});
  auto tupleSetB = TupleSet2::make(tableB);
  return tupleSetB;
}

}

#define SKIP_SUITE true

TEST_SUITE ("group" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("group-filescan-group-collate" * doctest::skip(false || SKIP_SUITE)) {
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

	auto group = Group::make("group", {"AA"}, {"sum"}, expressions2, 0);
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

TEST_CASE ("group-1xgroupcolumn" * doctest::skip(false || SKIP_SUITE)) {

  auto tupleSet1 = makeTupleSet1();
  auto tupleSet2 = makeTupleSet2();

  auto sumExpr = std::make_shared<Sum>("sum", cast(col("ab"), float64Type()));
  std::vector<std::shared_ptr<AggregationFunction>> expressions{sumExpr};

  GroupKernel groupKernel1({"aa"}, expressions);
  GroupKernel2 groupKernel2({"aa"}, {"sum"}, expressions);

  groupKernel1.onTuple(*tupleSet1);
  groupKernel1.onTuple(*tupleSet2);
  auto groupedTupleSet1 = groupKernel1.group();

  REQUIRE(groupKernel2.group(*tupleSet1).has_value());
  REQUIRE(groupKernel2.group(*tupleSet2).has_value());
  auto groupedTupleSet2 = groupKernel2.finalise();
  REQUIRE(groupedTupleSet2.has_value());

  SPDLOG_INFO("Output 1:\n{}", groupedTupleSet1->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));
  SPDLOG_INFO("Output 2:\n{}", groupedTupleSet2.value()->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));
}

TEST_CASE ("group-2xgroupcolumn" * doctest::skip(false || SKIP_SUITE)) {

  auto tupleSet1 = makeTupleSet1();
  auto tupleSet2 = makeTupleSet2();

  auto sumExpr = std::make_shared<Sum>("sum", cast(col("ab"), float64Type()));
  std::vector<std::shared_ptr<AggregationFunction>> expressions{sumExpr};

  GroupKernel groupKernel1({"aa", "ab"}, expressions);
  GroupKernel2 groupKernel2({"aa", "ab"}, {"sum"}, expressions);

  groupKernel1.onTuple(*tupleSet1);
  groupKernel1.onTuple(*tupleSet2);
  auto groupedTupleSet1 = groupKernel1.group();

	  REQUIRE(groupKernel2.group(*tupleSet1).has_value());
	  REQUIRE(groupKernel2.group(*tupleSet2).has_value());
  auto groupedTupleSet2 = groupKernel2.finalise();
	  REQUIRE(groupedTupleSet2.has_value());

  SPDLOG_INFO("Output 1:\n{}", groupedTupleSet1->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));
  SPDLOG_INFO("Output 2:\n{}", groupedTupleSet2.value()->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));
}

TEST_CASE ("group-0xgroupcolumn" * doctest::skip(false || SKIP_SUITE)) {

  auto tupleSet1 = makeTupleSet1();
  auto tupleSet2 = makeTupleSet2();

  auto sumExpr = std::make_shared<Sum>("sum", cast(col("ab"), float64Type()));
  std::vector<std::shared_ptr<AggregationFunction>> expressions{sumExpr};

  GroupKernel groupKernel1({}, expressions);
  GroupKernel2 groupKernel2({}, {"sum"}, expressions);

  groupKernel1.onTuple(*tupleSet1);
  groupKernel1.onTuple(*tupleSet2);
  auto groupedTupleSet1 = groupKernel1.group();

	  REQUIRE(groupKernel2.group(*tupleSet1).has_value());
	  REQUIRE(groupKernel2.group(*tupleSet2).has_value());
  auto groupedTupleSet2 = groupKernel2.finalise();
	  REQUIRE(groupedTupleSet2.has_value());

  SPDLOG_INFO("Output 1:\n{}", groupedTupleSet1->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));
  SPDLOG_INFO("Output 2:\n{}", groupedTupleSet2.value()->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));
}

TEST_CASE ("group-0xaggregates" * doctest::skip(false || SKIP_SUITE)) {

  auto tupleSet1 = makeTupleSet1();
  auto tupleSet2 = makeTupleSet2();

  std::vector<std::shared_ptr<AggregationFunction>> expressions{};

  GroupKernel groupKernel1({"aa"}, expressions);
  GroupKernel2 groupKernel2({"aa"}, {"sum"}, expressions);

  groupKernel1.onTuple(*tupleSet1);
  groupKernel1.onTuple(*tupleSet2);
  auto groupedTupleSet1 = groupKernel1.group();

	  REQUIRE(groupKernel2.group(*tupleSet1).has_value());
	  REQUIRE(groupKernel2.group(*tupleSet2).has_value());
  auto groupedTupleSet2 = groupKernel2.finalise();
	  REQUIRE(groupedTupleSet2.has_value());

  SPDLOG_INFO("Output 1:\n{}", groupedTupleSet1->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));
  SPDLOG_INFO("Output 2:\n{}", groupedTupleSet2.value()->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));
}

TEST_CASE ("group-2xfinalise" * doctest::skip(false || SKIP_SUITE)) {

  auto tupleSet1 = makeTupleSet1();
  auto tupleSet2 = makeTupleSet2();

  auto sumExpr = std::make_shared<Sum>("sum", cast(col("ab"), float64Type()));
  std::vector<std::shared_ptr<AggregationFunction>> expressions{sumExpr};

  GroupKernel2 groupKernel2({"aa"}, {"sum"}, expressions);

	  REQUIRE(groupKernel2.group(*tupleSet1).has_value());
	  REQUIRE(groupKernel2.group(*tupleSet2).has_value());
  auto groupedTupleSet21 = groupKernel2.finalise();
	  REQUIRE(groupedTupleSet21.has_value());
  auto groupedTupleSet22 = groupKernel2.finalise();
	  REQUIRE(groupedTupleSet22.has_value());

  SPDLOG_INFO("Output 1:\n{}", groupedTupleSet21.value()->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));
  SPDLOG_INFO("Output 2:\n{}", groupedTupleSet22.value()->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));
}

TEST_CASE ("group-uppercase" * doctest::skip(false || SKIP_SUITE)) {

  auto tupleSet1 = makeTupleSet1();
  auto tupleSet2 = makeTupleSet2();

  auto sumExpr = std::make_shared<Sum>("sum", cast(col("AB"), float64Type()));
  std::vector<std::shared_ptr<AggregationFunction>> expressions{sumExpr};

  GroupKernel2 groupKernel2({"AA"}, {"sum"}, expressions);

	  REQUIRE(groupKernel2.group(*tupleSet1).has_value());
	  REQUIRE(groupKernel2.group(*tupleSet2).has_value());
  auto groupedTupleSet2 = groupKernel2.finalise();
	  REQUIRE(groupedTupleSet2.has_value());

  SPDLOG_INFO("Output 2:\n{}", groupedTupleSet2.value()->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));
}

TEST_CASE ("group-empty" * doctest::skip(false || SKIP_SUITE)) {

  auto tupleSetE = makeEmptyTupleSet();
  auto tupleSet1 = makeTupleSet1();
  auto tupleSet2 = makeTupleSet2();

  auto sumExpr = std::make_shared<Sum>("sum", cast(col("ab"), float64Type()));
  std::vector<std::shared_ptr<AggregationFunction>> expressions{sumExpr};

  GroupKernel2 groupKernel2({"aa"}, {"sum"}, expressions);

	  REQUIRE(groupKernel2.group(*tupleSetE).has_value());
	  REQUIRE(groupKernel2.group(*tupleSet1).has_value());
	  REQUIRE(groupKernel2.group(*tupleSet2).has_value());
  auto groupedTupleSet2 = groupKernel2.finalise();
	  REQUIRE(groupedTupleSet2.has_value());

  SPDLOG_INFO("Output 1:\n{}", groupedTupleSet2.value()->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));
}

TEST_CASE ("group-undefined" * doctest::skip(false || SKIP_SUITE)) {

  auto tupleSetU = makeUndefinedTupleSet();
  auto tupleSet1 = makeTupleSet1();
  auto tupleSet2 = makeTupleSet2();

  auto sumExpr = std::make_shared<Sum>("sum", cast(col("ab"), float64Type()));
  std::vector<std::shared_ptr<AggregationFunction>> expressions{sumExpr};

  GroupKernel2 groupKernel2({"aa"}, {"sum"}, expressions);

	  REQUIRE_FALSE(groupKernel2.group(*tupleSetU).has_value());
}

}