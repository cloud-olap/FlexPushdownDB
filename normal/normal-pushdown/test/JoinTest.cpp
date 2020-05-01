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
}
