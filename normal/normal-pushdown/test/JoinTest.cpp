//
// Created by matt on 5/3/20.
//

#include <memory>

#include <doctest/doctest.h>

#include <normal/core/ATTIC/Normal.h>
#include <normal/pushdown/collate/Collate.h>
#include <normal/core/OperatorManager.h>
#include <normal/pushdown/file/FileScan.h>
#include <normal/pushdown/join/HashJoinBuild.h>
#include <normal/pushdown/join/HashJoinProbe.h>
#include "TestUtil.h"

using namespace normal::core::graph;
using namespace normal::pushdown;
using namespace normal::pushdown::join;
using namespace normal::pushdown::test;

#define SKIP_SUITE true

TEST_SUITE ("join-test" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("join-test-filescan-join-collate" * doctest::skip(false || SKIP_SUITE)) {

  {
	auto n = Normal::start();
	auto g = n->createQuery();

	auto aFile = filesystem::absolute("data/join/a.csv");
	auto numBytesAFile = filesystem::file_size(aFile);

	auto bFile = filesystem::absolute("data/join/b.csv");
	auto numBytesBFile = filesystem::file_size(bFile);

	auto aScan = FileScan::make("fileScanA",
								"data/join/a.csv",
								std::vector<std::string>{"AA", "AB", "AC"},
								0,
								numBytesAFile,
								g->getId(),
								true);
	g->put(aScan);
	auto bScan = FileScan::make("fileScanB",
								"data/join/b.csv",
								std::vector<std::string>{"BA", "BB", "BC"},
								0,
								numBytesBFile,
								g->getId(),
								true);
	g->put(bScan);
	auto joinBuild = HashJoinBuild::create("join-build", "AA");
	g->put(joinBuild);
	auto joinProbe = std::make_shared<HashJoinProbe>("join-probe",
													 JoinPredicate::create("AA", "BA"), std::set<std::string>{"AA", "BA"}, 0);
	g->put(joinProbe);
	auto collate = std::make_shared<Collate>("collate", g->getId());
	g->put(collate);

	aScan->produce(joinBuild);
	joinBuild->consume(aScan);

	joinBuild->produce(joinProbe);
	joinProbe->consume(joinBuild);

	bScan->produce(joinProbe);
	joinProbe->consume(bScan);

	joinProbe->produce(collate);
	collate->consume(joinProbe);

	TestUtil::writeExecutionPlan(*g);

	auto expectedTupleSet = g->execute();
		REQUIRE(expectedTupleSet.has_value());
	auto tupleSet = expectedTupleSet.value();

	SPDLOG_INFO("Output:\n{}", tupleSet->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));

		CHECK(tupleSet->numRows() == 4);
		CHECK(tupleSet->numColumns() == 6);

	/*
	 * FIXME: The following assumes the output is produced in a specific order but this shouldn't necessarily
	 *  be assumed. Will only be able to check the properly once we have a sort operator
	 */
	auto columnAA = tupleSet->getColumnByName("AA").value();
		CHECK(columnAA->element(0).value()->value<std::string>() == "10");
		CHECK(columnAA->element(1).value()->value<std::string>() == "10");
		CHECK(columnAA->element(2).value()->value<std::string>() == "10");
		CHECK(columnAA->element(3).value()->value<std::string>() == "10");

	auto columnAB = tupleSet->getColumnByName("AB").value();
		CHECK(columnAB->element(0).value()->value<std::string>() == "14");
		CHECK(columnAB->element(1).value()->value<std::string>() == "13");
		CHECK(columnAB->element(2).value()->value<std::string>() == "14");
		CHECK(columnAB->element(3).value()->value<std::string>() == "13");

	auto columnAC = tupleSet->getColumnByName("AC").value();
		CHECK(columnAC->element(0).value()->value<std::string>() == "17");
		CHECK(columnAC->element(1).value()->value<std::string>() == "16");
		CHECK(columnAC->element(2).value()->value<std::string>() == "17");
		CHECK(columnAC->element(3).value()->value<std::string>() == "16");

	auto columnBA = tupleSet->getColumnByName("BA").value();
		CHECK(columnBA->element(0).value()->value<std::string>() == "10");
		CHECK(columnBA->element(1).value()->value<std::string>() == "10");
		CHECK(columnBA->element(2).value()->value<std::string>() == "10");
		CHECK(columnBA->element(3).value()->value<std::string>() == "10");

	auto columnBB = tupleSet->getColumnByName("BB").value();
		CHECK(columnBB->element(0).value()->value<std::string>() == "23");
		CHECK(columnBB->element(1).value()->value<std::string>() == "23");
		CHECK(columnBB->element(2).value()->value<std::string>() == "25");
		CHECK(columnBB->element(3).value()->value<std::string>() == "25");

	auto columnBC = tupleSet->getColumnByName("BC").value();
		CHECK(columnBC->element(0).value()->value<std::string>() == "26");
		CHECK(columnBC->element(1).value()->value<std::string>() == "26");
		CHECK(columnBC->element(2).value()->value<std::string>() == "28");
		CHECK(columnBC->element(3).value()->value<std::string>() == "28");

	n->stop();
  }
	  REQUIRE_EQ(::arrow::default_memory_pool()->bytes_allocated(), 0);
}

auto makeEmptyTupleSetA() {
  auto schemaA = ::arrow::schema({::arrow::field("aa", ::arrow::int64()),
								  ::arrow::field("ab", ::arrow::int64()),
								  ::arrow::field("ac", ::arrow::int64())});

  auto arrayAA1 = Arrays::make<arrow::Int64Type>({}).value();
  auto arrayAA2 = Arrays::make<arrow::Int64Type>({}).value();
  auto arrayAA = std::make_shared<arrow::ChunkedArray>(arrow::ArrayVector{arrayAA1, arrayAA2});
  auto arrayAB1 = Arrays::make<arrow::Int64Type>({}).value();
  auto arrayAB2 = Arrays::make<arrow::Int64Type>({}).value();
  auto arrayAB = std::make_shared<arrow::ChunkedArray>(arrow::ArrayVector{arrayAB1, arrayAB2});
  auto arrayAC1 = Arrays::make<arrow::Int64Type>({}).value();
  auto arrayAC2 = Arrays::make<arrow::Int64Type>({}).value();
  auto arrayAC = std::make_shared<arrow::ChunkedArray>(arrow::ArrayVector{arrayAC1, arrayAC2});
  auto tableA = arrow::Table::Make(schemaA, {arrayAA, arrayAB, arrayAC});
  auto tupleSetA = TupleSet2::make(tableA);
  return tupleSetA;
}

auto makeEmptyTupleSetB() {
  auto schemaB = ::arrow::schema({::arrow::field("ba", ::arrow::int64()),
								  ::arrow::field("bb", ::arrow::int64()),
								  ::arrow::field("bc", ::arrow::int64())});

  auto arrayBA1 = Arrays::make<arrow::Int64Type>({}).value();
  auto arrayBA2 = Arrays::make<arrow::Int64Type>({}).value();
  auto arrayBA = std::make_shared<arrow::ChunkedArray>(arrow::ArrayVector{arrayBA1, arrayBA2});
  auto arrayBB1 = Arrays::make<arrow::Int64Type>({}).value();
  auto arrayBB2 = Arrays::make<arrow::Int64Type>({}).value();
  auto arrayBB = std::make_shared<arrow::ChunkedArray>(arrow::ArrayVector{arrayBB1, arrayBB2});
  auto arrayBC1 = Arrays::make<arrow::Int64Type>({}).value();
  auto arrayBC2 = Arrays::make<arrow::Int64Type>({}).value();
  auto arrayBC = std::make_shared<arrow::ChunkedArray>(arrow::ArrayVector{arrayBC1, arrayBC2});
  auto tableB = arrow::Table::Make(schemaB, {arrayBA, arrayBB, arrayBC});
  auto tupleSetB = TupleSet2::make(tableB);
  return tupleSetB;
}

auto makeTupleSetA() {
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

auto makeTupleSetB() {
  auto schemaB = ::arrow::schema({::arrow::field("ba", ::arrow::int64()),
								  ::arrow::field("bb", ::arrow::int64()),
								  ::arrow::field("bc", ::arrow::int64())});

  auto arrayBA1 = Arrays::make<arrow::Int64Type>({1, 2}).value();
  auto arrayBA2 = Arrays::make<arrow::Int64Type>({3}).value();
  auto arrayBA = std::make_shared<arrow::ChunkedArray>(arrow::ArrayVector{arrayBA1, arrayBA2});
  auto arrayBB1 = Arrays::make<arrow::Int64Type>({4, 5}).value();
  auto arrayBB2 = Arrays::make<arrow::Int64Type>({6}).value();
  auto arrayBB = std::make_shared<arrow::ChunkedArray>(arrow::ArrayVector{arrayBB1, arrayBB2});
  auto arrayBC1 = Arrays::make<arrow::Int64Type>({7, 8}).value();
  auto arrayBC2 = Arrays::make<arrow::Int64Type>({9}).value();
  auto arrayBC = std::make_shared<arrow::ChunkedArray>(arrow::ArrayVector{arrayBC1, arrayBC2});
  auto tableB = arrow::Table::Make(schemaB, {arrayBA, arrayBB, arrayBC});
  auto tupleSetB = TupleSet2::make(tableB);
  return tupleSetB;
}

TEST_CASE ("join-test-non-existent-build-column" * doctest::skip(false || SKIP_SUITE)) {

  auto tupleSetA = makeTupleSetA();

  HashJoinBuildKernel2 buildKernel("NON_EXISTENT_COLUMN_NAME");

  // This should fail
  auto buildKernelPutResult = buildKernel.put(tupleSetA);
	  REQUIRE_FALSE(buildKernelPutResult.has_value());
}

TEST_CASE ("join-test-non-existent-probe-left-column" * doctest::skip(false || SKIP_SUITE)) {
  auto tupleSetA = makeTupleSetA();

  HashJoinBuildKernel2 buildKernel("aa");
  HashJoinProbeKernel2 probeKernel(JoinPredicate::create("NON_EXISTENT_COLUMN_NAME", "UNUSED"), {});

  // This should succeed
  auto buildKernelPutResult = buildKernel.put(tupleSetA);
	  REQUIRE(buildKernelPutResult.has_value());

  // This should succeed
  auto expectedTupleSetIndex = buildKernel.getTupleSetIndex();
	  REQUIRE(expectedTupleSetIndex.has_value());
  auto tupleSetIndex = expectedTupleSetIndex.value();

  // This should fail
  auto probeKernelPutBuildIndexResult = probeKernel.putBuildTupleSetIndex(tupleSetIndex);
	  REQUIRE_FALSE(probeKernelPutBuildIndexResult);
}

TEST_CASE ("join-test-non-existent-probe-right-column" * doctest::skip(false || SKIP_SUITE)) {
  auto tupleSetA = makeTupleSetA();

  HashJoinProbeKernel2 probeKernel(JoinPredicate::create("UNUSED", "NON_EXISTENT_COLUMN_NAME"), {});

  // This should fail
  auto probeKernelPutProbeResult = probeKernel.putProbeTupleSet(tupleSetA);
	  REQUIRE_FALSE(probeKernelPutProbeResult);
}

TEST_CASE ("join-test-empty-build-tupleset" * doctest::skip(false || SKIP_SUITE)) {

  auto tupleSetA = makeEmptyTupleSetA();
  auto tupleSetB = makeTupleSetB();

  HashJoinBuildKernel2 buildKernel("aa");
  HashJoinProbeKernel2 probeKernel(JoinPredicate::create("aa", "ba"), {"aa", "ba"});

  // This should succeed
  auto buildKernelPutResult = buildKernel.put(tupleSetA);
	  REQUIRE(buildKernelPutResult.has_value());

  // This should succeed
  auto expectedTupleSetIndex = buildKernel.getTupleSetIndex();
	  REQUIRE(expectedTupleSetIndex.has_value());
  auto tupleSetIndex = expectedTupleSetIndex.value();

  // This should succeed
  auto probeKernelPutBuildIndexResult = probeKernel.putBuildTupleSetIndex(tupleSetIndex);
	  REQUIRE(probeKernelPutBuildIndexResult);

  // This should succeed
  auto probeKernelPutProbeResult = probeKernel.joinProbeTupleSet(tupleSetB);
	  REQUIRE(probeKernelPutProbeResult);

  // This should succeed
  auto joinResult = probeKernel.getBuffer();
	  REQUIRE(joinResult);
  auto tupleSetJoined = joinResult.value();

  // Joined tupleset should be empty
	  REQUIRE_EQ(tupleSetJoined->numColumns(), 6);
	  REQUIRE_EQ(tupleSetJoined->numRows(), 0);
}

TEST_CASE ("join-test-empty-probe-tupleset" * doctest::skip(false || SKIP_SUITE)) {

  auto tupleSetA = makeTupleSetA();
  auto tupleSetB = makeEmptyTupleSetB();

  HashJoinBuildKernel2 buildKernel("aa");
  HashJoinProbeKernel2 probeKernel(JoinPredicate::create("aa", "ba"), {"aa", "ba"});

  // This should succeed
  auto buildKernelPutResult = buildKernel.put(tupleSetA);
	  REQUIRE(buildKernelPutResult.has_value());

  // This should succeed
  auto expectedTupleSetIndex = buildKernel.getTupleSetIndex();
	  REQUIRE(expectedTupleSetIndex.has_value());
  auto tupleSetIndex = expectedTupleSetIndex.value();

  // This should succeed
  auto probeKernelPutBuildIndexResult = probeKernel.putBuildTupleSetIndex(tupleSetIndex);
	  REQUIRE(probeKernelPutBuildIndexResult);

  // This should succeed
  auto probeKernelPutProbeResult = probeKernel.joinProbeTupleSet(tupleSetB);
	  REQUIRE(probeKernelPutProbeResult);

  // This should succeed
  auto joinResult = probeKernel.getBuffer();
	  REQUIRE(joinResult);
  auto tupleSetJoined = joinResult.value();

  // Joined tupleset should be empty
	  REQUIRE_EQ(tupleSetJoined->numColumns(), 6);
	  REQUIRE_EQ(tupleSetJoined->numRows(), 0);
}

TEST_CASE ("join-test-default" * doctest::skip(false || SKIP_SUITE)) {
  auto tupleSetA = makeTupleSetA();
  auto tupleSetB = makeTupleSetB();

  HashJoinBuildKernel2 buildKernel("aa");
  HashJoinProbeKernel2 probeKernel(JoinPredicate::create("aa", "ba"), {"aa", "ba"});

  auto buildKernelPutResult = buildKernel.put(tupleSetA);
	  REQUIRE(buildKernelPutResult.has_value());

  auto expectedTupleSetIndex = buildKernel.getTupleSetIndex();
	  REQUIRE(expectedTupleSetIndex.has_value());
  auto tupleSetIndex = expectedTupleSetIndex.value();

  auto probeKernelPutBuildIndexResult = probeKernel.putBuildTupleSetIndex(tupleSetIndex);
	  REQUIRE(probeKernelPutBuildIndexResult);

  auto probeKernelPutProbeTupleResult = probeKernel.joinProbeTupleSet(tupleSetB);
	  REQUIRE(probeKernelPutProbeTupleResult);

  auto joinResult = probeKernel.getBuffer();
	  REQUIRE(joinResult);

    SPDLOG_DEBUG(joinResult.value()->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));
}

TEST_CASE ("join-test-2xbatches" * doctest::skip(false || SKIP_SUITE)) {
  auto tupleSetA1 = makeTupleSetA();
  auto tupleSetA2 = makeTupleSetA();
  auto tupleSetB1 = makeTupleSetB();
  auto tupleSetB2 = makeTupleSetB();

//  SPDLOG_DEBUG(tupleSetA1->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));
//  SPDLOG_DEBUG(tupleSetA2->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));
//  SPDLOG_DEBUG(tupleSetB1->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));
//  SPDLOG_DEBUG(tupleSetB2->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));

  HashJoinBuildKernel2 buildKernel("aa");
  HashJoinProbeKernel2 probeKernel(JoinPredicate::create("aa", "ba"), {"aa", "ba"});

  auto buildKernelPutResult1 = buildKernel.put(tupleSetA1);
	  REQUIRE(buildKernelPutResult1.has_value());

  auto buildKernelPutResult2 = buildKernel.put(tupleSetA2);
	  REQUIRE(buildKernelPutResult2.has_value());

  auto expectedTupleSetIndex = buildKernel.getTupleSetIndex();
	  REQUIRE(expectedTupleSetIndex.has_value());
  auto tupleSetIndex = expectedTupleSetIndex.value();

  auto probeKernelPutBuildIndexResult = probeKernel.putBuildTupleSetIndex(tupleSetIndex);
	  REQUIRE(probeKernelPutBuildIndexResult);

  auto probeKernelPutProbeTupleResult1 = probeKernel.putProbeTupleSet(tupleSetB1);
	  REQUIRE(probeKernelPutProbeTupleResult1);

  auto probeKernelPutProbeTupleResult2 = probeKernel.joinProbeTupleSet(tupleSetB2);
	  REQUIRE(probeKernelPutProbeTupleResult2);

  auto joinResult = probeKernel.getBuffer();
	  REQUIRE(joinResult);

  SPDLOG_DEBUG(joinResult.value()->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));

}

}