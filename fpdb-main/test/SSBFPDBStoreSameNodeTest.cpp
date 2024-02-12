//
// Created by Yifei Yang on 12/21/22.
//

#include <doctest/doctest.h>
#include <fpdb/store/server/Server.hpp>
#include <fpdb/store/client/FPDBStoreClientConfig.h>
#include <fpdb/executor/physical/Globals.h>
#include "TestUtil.h"
#include "Globals.h"

/**
 * TPCH test (single compute node, also as the single FPDB store node)
 *
 * Start Calcite server before running this
 */
namespace fpdb::main::test {

#define SKIP_SUITE false

TEST_SUITE ("ssb-sf0.01-fpdb-store-same-node-parquet-pullup" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("ssb-sf0.01-fpdb-store-same-node-parquet-pullup-1.1" * doctest::skip(false || SKIP_SUITE)) {
  TestUtil::startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("ssb-sf0.01/parquet/",
                                            {"ssb/original/1.1.sql"},
                                            PARALLEL_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  TestUtil::stopFPDBStoreServer();
}

TEST_CASE ("ssb-sf0.01-fpdb-store-same-node-parquet-pullup-1.2" * doctest::skip(false || SKIP_SUITE)) {
  TestUtil::startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("ssb-sf0.01/parquet/",
                                            {"ssb/original/1.2.sql"},
                                            PARALLEL_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  TestUtil::stopFPDBStoreServer();
}

TEST_CASE ("ssb-sf0.01-fpdb-store-same-node-parquet-pullup-1.3" * doctest::skip(false || SKIP_SUITE)) {
  TestUtil::startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("ssb-sf0.01/parquet/",
                                            {"ssb/original/1.3.sql"},
                                            PARALLEL_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  TestUtil::stopFPDBStoreServer();
}

TEST_CASE ("ssb-sf0.01-fpdb-store-same-node-parquet-pullup-2.1" * doctest::skip(false || SKIP_SUITE)) {
  TestUtil::startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("ssb-sf0.01/parquet/",
                                            {"ssb/original/2.1.sql"},
                                            PARALLEL_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  TestUtil::stopFPDBStoreServer();
}

TEST_CASE ("ssb-sf0.01-fpdb-store-same-node-parquet-pullup-2.2" * doctest::skip(false || SKIP_SUITE)) {
  TestUtil::startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("ssb-sf0.01/parquet/",
                                            {"ssb/original/2.2.sql"},
                                            PARALLEL_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  TestUtil::stopFPDBStoreServer();
}

TEST_CASE ("ssb-sf0.01-fpdb-store-same-node-parquet-pullup-2.3" * doctest::skip(false || SKIP_SUITE)) {
  TestUtil::startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("ssb-sf0.01/parquet/",
                                            {"ssb/original/2.3.sql"},
                                            PARALLEL_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  TestUtil::stopFPDBStoreServer();
}

TEST_CASE ("ssb-sf0.01-fpdb-store-same-node-parquet-pullup-3.1" * doctest::skip(false || SKIP_SUITE)) {
  TestUtil::startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("ssb-sf0.01/parquet/",
                                            {"ssb/original/3.1.sql"},
                                            PARALLEL_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  TestUtil::stopFPDBStoreServer();
}

TEST_CASE ("ssb-sf0.01-fpdb-store-same-node-parquet-pullup-3.2" * doctest::skip(false || SKIP_SUITE)) {
  TestUtil::startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("ssb-sf0.01/parquet/",
                                            {"ssb/original/3.2.sql"},
                                            PARALLEL_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  TestUtil::stopFPDBStoreServer();
}

TEST_CASE ("ssb-sf0.01-fpdb-store-same-node-parquet-pullup-3.3" * doctest::skip(false || SKIP_SUITE)) {
  TestUtil::startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("ssb-sf0.01/parquet/",
                                            {"ssb/original/3.3.sql"},
                                            PARALLEL_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  TestUtil::stopFPDBStoreServer();
}

TEST_CASE ("ssb-sf0.01-fpdb-store-same-node-parquet-pullup-3.4" * doctest::skip(false || SKIP_SUITE)) {
  TestUtil::startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("ssb-sf0.01/parquet/",
                                            {"ssb/original/3.4.sql"},
                                            PARALLEL_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  TestUtil::stopFPDBStoreServer();
}

TEST_CASE ("ssb-sf0.01-fpdb-store-same-node-parquet-pullup-4.1" * doctest::skip(false || SKIP_SUITE)) {
  TestUtil::startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("ssb-sf0.01/parquet/",
                                            {"ssb/original/4.1.sql"},
                                            PARALLEL_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  TestUtil::stopFPDBStoreServer();
}

TEST_CASE ("ssb-sf0.01-fpdb-store-same-node-parquet-pullup-4.2" * doctest::skip(false || SKIP_SUITE)) {
  TestUtil::startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("ssb-sf0.01/parquet/",
                                            {"ssb/original/4.2.sql"},
                                            PARALLEL_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  TestUtil::stopFPDBStoreServer();
}

TEST_CASE ("ssb-sf0.01-fpdb-store-same-node-parquet-pullup-4.3" * doctest::skip(false || SKIP_SUITE)) {
  TestUtil::startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("ssb-sf0.01/parquet/",
                                            {"ssb/original/4.3.sql"},
                                            PARALLEL_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  TestUtil::stopFPDBStoreServer();
}

}

TEST_SUITE ("ssb-sf0.01-fpdb-store-same-node-parquet-pushdown-only" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("ssb-sf0.01-fpdb-store-same-node-parquet-pushdown-only-1.1" * doctest::skip(false || SKIP_SUITE)) {
  TestUtil::startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("ssb-sf0.01/parquet/",
                                            {"ssb/original/1.1.sql"},
                                            PARALLEL_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
  TestUtil::stopFPDBStoreServer();
}

TEST_CASE ("ssb-sf0.01-fpdb-store-same-node-parquet-pushdown-only-1.2" * doctest::skip(false || SKIP_SUITE)) {
  TestUtil::startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("ssb-sf0.01/parquet/",
                                            {"ssb/original/1.2.sql"},
                                            PARALLEL_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
  TestUtil::stopFPDBStoreServer();
}

TEST_CASE ("ssb-sf0.01-fpdb-store-same-node-parquet-pushdown-only-1.3" * doctest::skip(false || SKIP_SUITE)) {
  TestUtil::startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("ssb-sf0.01/parquet/",
                                            {"ssb/original/1.3.sql"},
                                            PARALLEL_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
  TestUtil::stopFPDBStoreServer();
}

TEST_CASE ("ssb-sf0.01-fpdb-store-same-node-parquet-pushdown-only-2.1" * doctest::skip(false || SKIP_SUITE)) {
  TestUtil::startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("ssb-sf0.01/parquet/",
                                            {"ssb/original/2.1.sql"},
                                            PARALLEL_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
  TestUtil::stopFPDBStoreServer();
}

TEST_CASE ("ssb-sf0.01-fpdb-store-same-node-parquet-pushdown-only-2.2" * doctest::skip(false || SKIP_SUITE)) {
  TestUtil::startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("ssb-sf0.01/parquet/",
                                            {"ssb/original/2.2.sql"},
                                            PARALLEL_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
  TestUtil::stopFPDBStoreServer();
}

TEST_CASE ("ssb-sf0.01-fpdb-store-same-node-parquet-pushdown-only-2.3" * doctest::skip(false || SKIP_SUITE)) {
  TestUtil::startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("ssb-sf0.01/parquet/",
                                            {"ssb/original/2.3.sql"},
                                            PARALLEL_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
  TestUtil::stopFPDBStoreServer();
}

TEST_CASE ("ssb-sf0.01-fpdb-store-same-node-parquet-pushdown-only-3.1" * doctest::skip(false || SKIP_SUITE)) {
  TestUtil::startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("ssb-sf0.01/parquet/",
                                            {"ssb/original/3.1.sql"},
                                            PARALLEL_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
  TestUtil::stopFPDBStoreServer();
}

TEST_CASE ("ssb-sf0.01-fpdb-store-same-node-parquet-pushdown-only-3.2" * doctest::skip(false || SKIP_SUITE)) {
  TestUtil::startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("ssb-sf0.01/parquet/",
                                            {"ssb/original/3.2.sql"},
                                            PARALLEL_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
  TestUtil::stopFPDBStoreServer();
}

TEST_CASE ("ssb-sf0.01-fpdb-store-same-node-parquet-pushdown-only-3.3" * doctest::skip(false || SKIP_SUITE)) {
  TestUtil::startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("ssb-sf0.01/parquet/",
                                            {"ssb/original/3.3.sql"},
                                            PARALLEL_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
  TestUtil::stopFPDBStoreServer();
}

TEST_CASE ("ssb-sf0.01-fpdb-store-same-node-parquet-pushdown-only-3.4" * doctest::skip(false || SKIP_SUITE)) {
  TestUtil::startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("ssb-sf0.01/parquet/",
                                            {"ssb/original/3.4.sql"},
                                            PARALLEL_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
  TestUtil::stopFPDBStoreServer();
}

TEST_CASE ("ssb-sf0.01-fpdb-store-same-node-parquet-pushdown-only-4.1" * doctest::skip(false || SKIP_SUITE)) {
  TestUtil::startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("ssb-sf0.01/parquet/",
                                            {"ssb/original/4.1.sql"},
                                            PARALLEL_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
  TestUtil::stopFPDBStoreServer();
}

TEST_CASE ("ssb-sf0.01-fpdb-store-same-node-parquet-pushdown-only-4.2" * doctest::skip(false || SKIP_SUITE)) {
  TestUtil::startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("ssb-sf0.01/parquet/",
                                            {"ssb/original/4.2.sql"},
                                            PARALLEL_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
  TestUtil::stopFPDBStoreServer();
}

TEST_CASE ("ssb-sf0.01-fpdb-store-same-node-parquet-pushdown-only-4.3" * doctest::skip(false || SKIP_SUITE)) {
  TestUtil::startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("ssb-sf0.01/parquet/",
                                            {"ssb/original/4.3.sql"},
                                            PARALLEL_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
  TestUtil::stopFPDBStoreServer();
}

}

}
