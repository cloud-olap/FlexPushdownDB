//
// Created by matt on 5/3/20.
//

#include <memory>

#include <doctest/doctest.h>

#include <normal/pushdown/Collate.h>
#include <normal/core/OperatorManager.h>
#include <normal/core/graph/OperatorGraph.h>
#include <normal/pushdown/file/FileScan.h>
#include <normal/pushdown/join/HashJoinBuild.h>
#include <normal/pushdown/join/HashJoinProbe.h>
#include "TestUtil.h"

using namespace normal::core::graph;
using namespace normal::pushdown;
using namespace normal::pushdown::join;
using namespace normal::pushdown::test;

#define SKIP_SUITE true

TEST_SUITE ("join" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("filescan-join-collate" * doctest::skip(false || SKIP_SUITE)) {

  auto aFile = filesystem::absolute("data/join/a.csv");
  auto numBytesAFile = filesystem::file_size(aFile);

  auto bFile = filesystem::absolute("data/join/b.csv");
  auto numBytesBFile = filesystem::file_size(aFile);

  auto mgr = std::make_shared<normal::core::OperatorManager>();

  auto g = OperatorGraph::make(mgr);

  auto aScan = FileScan::make("fileScanA", "data/join/a.csv", std::vector<std::string>{"AA"}, 0, numBytesAFile, g->getId());
  auto bScan = FileScan::make("fileScanB", "data/join/b.csv", std::vector<std::string>{"BA"}, 0, numBytesBFile, g->getId());
  auto joinBuild = HashJoinBuild::create("join-build", "AA");
  auto joinProbe = std::make_shared<HashJoinProbe>("join-probe",
												   JoinPredicate::create("AA", "BA"));
  auto collate = std::make_shared<Collate>("collate", g->getId());

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

  TestUtil::writeExecutionPlan(*g);

  mgr->boot();

  mgr->start();
  mgr->join();

  auto tuples = collate->tuples();

  mgr->stop();

  auto tupleSet = TupleSet2::create(tuples);

  SPDLOG_INFO("Output:\n{}", tupleSet->showString(TupleSetShowOptions(TupleSetShowOrientation::RowOriented)));

	  CHECK(tupleSet->numRows() == 4);
	  CHECK(tupleSet->numColumns() == 6);

  /*
   * FIXME: The following assumes the output is produced in a specific order but this shouldn't necessarily
   *  be assumed. Will only be able to check the properly once we have a sort operator
   */
  auto columnAA = tupleSet->getColumnByName("AA").value();
	  CHECK(columnAA->element(0).value()->value<long>() == 10);
	  CHECK(columnAA->element(1).value()->value<long>() == 10);
	  CHECK(columnAA->element(2).value()->value<long>() == 10);
	  CHECK(columnAA->element(3).value()->value<long>() == 10);

  auto columnAB = tupleSet->getColumnByName("AB").value();
	  CHECK(columnAB->element(0).value()->value<long>() == 13);
	  CHECK(columnAB->element(1).value()->value<long>() == 14);
	  CHECK(columnAB->element(2).value()->value<long>() == 13);
	  CHECK(columnAB->element(3).value()->value<long>() == 14);

  auto columnAC = tupleSet->getColumnByName("AC").value();
	  CHECK(columnAC->element(0).value()->value<long>() == 16);
	  CHECK(columnAC->element(1).value()->value<long>() == 17);
	  CHECK(columnAC->element(2).value()->value<long>() == 16);
	  CHECK(columnAC->element(3).value()->value<long>() == 17);

  auto columnBA = tupleSet->getColumnByName("BA").value();
	  CHECK(columnBA->element(0).value()->value<long>() == 10);
	  CHECK(columnBA->element(1).value()->value<long>() == 10);
	  CHECK(columnBA->element(2).value()->value<long>() == 10);
	  CHECK(columnBA->element(3).value()->value<long>() == 10);

  auto columnBB = tupleSet->getColumnByName("BB").value();
	  CHECK(columnBB->element(0).value()->value<long>() == 23);
	  CHECK(columnBB->element(1).value()->value<long>() == 23);
	  CHECK(columnBB->element(2).value()->value<long>() == 25);
	  CHECK(columnBB->element(3).value()->value<long>() == 25);

  auto columnBC = tupleSet->getColumnByName("BC").value();
	  CHECK(columnBC->element(0).value()->value<long>() == 26);
	  CHECK(columnBC->element(1).value()->value<long>() == 26);
	  CHECK(columnBC->element(2).value()->value<long>() == 28);
	  CHECK(columnBC->element(3).value()->value<long>() == 28);
}

}