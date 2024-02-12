//
// Created by Yifei Yang on 12/13/22.
//

#include "AdaptPushdownTestUtil.h"
#include "TestUtil.h"
#include <fpdb/executor/physical/Globals.h>
#include <fpdb/executor/flight/FlightClients.h>
#include <fpdb/store/server/flight/Util.hpp>
#include <fpdb/store/server/flight/ClearAdaptPushdownMetricsCmd.hpp>
#include <fpdb/store/server/flight/SetAdaptPushdownCmd.hpp>
#include <fpdb/store/client/FPDBStoreClientConfig.h>
#include <arrow/flight/api.h>
#include <doctest/doctest.h>
#include "thread"

namespace fpdb::main::test {

void AdaptPushdownTestUtil::run_adapt_pushdown_benchmark_query(const std::string &schemaName,
                                                               const std::string &queryFileName,
                                                               const std::vector<int> &maxThreadsVec,
                                                               int parallelDegree,
                                                               bool startFPDBStore) {
  // save old configs
  bool oldEnableAdaptPushdown = fpdb::executor::physical::ENABLE_ADAPTIVE_PUSHDOWN;
  int oldMaxThreads = fpdb::store::server::flight::MaxThreads;

  // start fpdb-store if local
  if (startFPDBStore) {
    TestUtil::startFPDBStoreServer();
  }

  // run pullup and pushdown once as gandiva cache makes the subsequent runs faster than the first run
  std::cout << "Start run (pullup)" << std::endl;
  REQUIRE(TestUtil::e2eNoStartCalciteServer(schemaName,
                                            {queryFileName},
                                            parallelDegree,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pullupMode()));
  std::cout << "Start run (pushdown)" << std::endl;
  REQUIRE(TestUtil::e2eNoStartCalciteServer(schemaName,
                                            {queryFileName},
                                            parallelDegree,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));

  // collect adaptive pushdown metrics for pullup
  std::cout << "Collect metrics run (pullup)" << std::endl;
  TestUtil testUtil(schemaName,
                    {queryFileName},
                    parallelDegree,
                    false,
                    ObjStoreType::FPDB_STORE,
                    Mode::pullupMode());
  testUtil.setCollAdaptPushdownMetrics(true);
  REQUIRE_NOTHROW(testUtil.runTest());

  // collect adaptive pushdown metrics for pushdown
  std::cout << "Collect metrics run (pushdown)" << std::endl;
  testUtil = TestUtil(schemaName,
                      {queryFileName},
                      parallelDegree,
                      false,
                      ObjStoreType::FPDB_STORE,
                      Mode::pushdownOnlyMode());
  testUtil.setCollAdaptPushdownMetrics(true);
  REQUIRE_NOTHROW(testUtil.runTest());

  // measurement runs
  for (int maxThreads: maxThreadsVec) {
    std::cout << fmt::format("Max threads at storage side: {}\n", maxThreads) << std::endl;

    // pullup baseline run
    std::cout << "Pullup baseline run" << std::endl;
    testUtil = TestUtil(schemaName,
                        {queryFileName},
                        parallelDegree,
                        false,
                        ObjStoreType::FPDB_STORE,
                        Mode::pullupMode());
    set_pushdown_flags(false, maxThreads, !startFPDBStore);
    std::this_thread::sleep_for(1s);
    REQUIRE_NOTHROW(testUtil.runTest());

    // pushdown baseline run
    std::cout << "Pushdown baseline run" << std::endl;
    testUtil = TestUtil(schemaName,
                        {queryFileName},
                        parallelDegree,
                        false,
                        ObjStoreType::FPDB_STORE,
                        Mode::pushdownOnlyMode());
    std::this_thread::sleep_for(1s);
    REQUIRE_NOTHROW(testUtil.runTest());

    // adaptive pushdown test run
    std::cout << "Adaptive pushdown run" << std::endl;
    testUtil = TestUtil(schemaName,
                        {queryFileName},
                        parallelDegree,
                        false,
                        ObjStoreType::FPDB_STORE,
                        Mode::pushdownOnlyMode());
    set_pushdown_flags(true, maxThreads, !startFPDBStore);
    std::this_thread::sleep_for(1s);
    REQUIRE_NOTHROW(testUtil.runTest());
  }

  // restore flags and clear adaptive pushdown metrics
  set_pushdown_flags(oldEnableAdaptPushdown, oldMaxThreads, !startFPDBStore);
  send_cmd_to_storage(fpdb::store::server::flight::ClearAdaptPushdownMetricsCmd::make());

  // stop fpdb-store if local
  if (startFPDBStore) {
    TestUtil::stopFPDBStoreServer();
  }
}

void AdaptPushdownTestUtil::set_pushdown_flags(bool enableAdaptPushdown, int maxThreads,
                                               bool isFPDBStoreRemote) {
  fpdb::executor::physical::ENABLE_ADAPTIVE_PUSHDOWN = enableAdaptPushdown;
  fpdb::store::server::flight::MaxThreads = maxThreads;
  if (isFPDBStoreRemote) {
    send_cmd_to_storage(fpdb::store::server::flight::SetAdaptPushdownCmd::make(
            enableAdaptPushdown, maxThreads));
  }
}

void AdaptPushdownTestUtil::send_cmd_to_storage(const std::shared_ptr<fpdb::store::server::flight::CmdObject> &cmdObj) {
  auto expCmd = cmdObj->serialize(false);
  if (!expCmd.has_value()) {
    throw std::runtime_error(expCmd.error());
    return;
  }

  // send to each fpdb-store node
  auto fpdbStoreClientConfig = fpdb::store::client::FPDBStoreClientConfig::parseFPDBStoreClientConfig();
  for (const auto &host: fpdbStoreClientConfig->getHosts()) {
    auto client = flight::GlobalFlightClients.getFlightClient(host, fpdbStoreClientConfig->getFlightPort());
    auto descriptor = ::arrow::flight::FlightDescriptor::Command(*expCmd);
    std::unique_ptr<arrow::flight::FlightStreamWriter> writer;
    std::unique_ptr<arrow::flight::FlightMetadataReader> metadataReader;
    auto status = client->DoPut(descriptor, nullptr, &writer, &metadataReader);
    if (!status.ok()) {
      throw std::runtime_error(status.message());
      return;
    }
    status = writer->Close();
    if (!status.ok()) {
      throw std::runtime_error(status.message());
      return;
    }
  }
}

}
