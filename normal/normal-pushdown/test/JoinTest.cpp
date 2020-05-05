//
// Created by matt on 5/3/20.
//

#include <memory>

#include <doctest/doctest.h>

#include <normal/pushdown/Collate.h>
#include <normal/core/OperatorManager.h>
#include <normal/pushdown/FileScan.h>
#include <normal/test/TestUtil.h>
#include <normal/pushdown/join/HashJoinBuild.h>
#include <normal/pushdown/join/HashJoinProbe.h>

using namespace normal::pushdown;
using namespace normal::pushdown::join;

TEST_CASE ("filescan-join-collate" * doctest::skip(false)) {

  auto mgr = std::make_shared<normal::core::OperatorManager>();

  auto aScan = std::make_shared<FileScan>("fileScanA", "data/join/a.csv");
  auto bScan = std::make_shared<FileScan>("fileScanB", "data/join/b.csv");
  auto joinBuild = HashJoinBuild::create("join-build", "AA");
  auto joinProbe = std::make_shared<HashJoinProbe>("join-probe",
												   JoinPredicate::create("AA", "BA"));
  auto collate = std::make_shared<Collate>("collate");

  aScan->produce(joinBuild);
  joinBuild->consume(aScan);

  joinBuild->produce(joinProbe);
  joinProbe->consume(joinBuild);

  bScan->produce(joinProbe);
  joinProbe->consume(bScan);

  joinProbe->produce(collate);
  collate->consume(joinProbe);

  mgr->put(aScan);
  mgr->put(bScan);
  mgr->put(joinBuild);
  mgr->put(joinProbe);
  mgr->put(collate);

  normal::test::TestUtil::writeExecutionPlan(*mgr);

  mgr->boot();

  mgr->start();
  mgr->join();

  auto tuples = collate->tuples();

  SPDLOG_INFO("Output:\n{}", tuples->toString());

  mgr->stop();

	  CHECK(tuples->numRows() == 4);
	  CHECK(tuples->numColumns() == 6);

	  /*
	   * FIXME: The following assumes the output is produced in a specific order but this shouldn't necessarily
	   *  be assumed. Will only be able to check the properly once we have a sort operator
	   *
	   * FIXME: Still using old tuple set class so column names aren't canonicalized, Need to use lower case for now.
	   */
	  CHECK(tuples->value<arrow::Int64Type>("aa", 0).value() == 10);
	  CHECK(tuples->value<arrow::Int64Type>("aa", 1).value() == 10);
	  CHECK(tuples->value<arrow::Int64Type>("aa", 2).value() == 10);
	  CHECK(tuples->value<arrow::Int64Type>("aa", 0).value() == 10);

	  CHECK(tuples->value<arrow::Int64Type>("ab", 0).value() == 13);
	  CHECK(tuples->value<arrow::Int64Type>("ab", 1).value() == 14);
	  CHECK(tuples->value<arrow::Int64Type>("ab", 2).value() == 13);
	  CHECK(tuples->value<arrow::Int64Type>("ab", 3).value() == 14);

	  CHECK(tuples->value<arrow::Int64Type>("ac", 0).value() == 16);
	  CHECK(tuples->value<arrow::Int64Type>("ac", 1).value() == 17);
	  CHECK(tuples->value<arrow::Int64Type>("ac", 2).value() == 16);
	  CHECK(tuples->value<arrow::Int64Type>("ac", 3).value() == 17);

	  CHECK(tuples->value<arrow::Int64Type>("ba", 0).value() == 10);
	  CHECK(tuples->value<arrow::Int64Type>("ba", 1).value() == 10);
	  CHECK(tuples->value<arrow::Int64Type>("ba", 2).value() == 10);
	  CHECK(tuples->value<arrow::Int64Type>("ba", 3).value() == 10);

	  CHECK(tuples->value<arrow::Int64Type>("bb", 0).value() == 23);
	  CHECK(tuples->value<arrow::Int64Type>("bb", 1).value() == 23);
	  CHECK(tuples->value<arrow::Int64Type>("bb", 2).value() == 25);
	  CHECK(tuples->value<arrow::Int64Type>("bb", 3).value() == 25);

	  CHECK(tuples->value<arrow::Int64Type>("bc", 0).value() == 26);
	  CHECK(tuples->value<arrow::Int64Type>("bc", 1).value() == 26);
	  CHECK(tuples->value<arrow::Int64Type>("bc", 2).value() == 28);
	  CHECK(tuples->value<arrow::Int64Type>("bc", 3).value() == 28);
}