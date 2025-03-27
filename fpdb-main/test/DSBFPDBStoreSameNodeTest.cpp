//
// Created by Yifei Yang on 12/5/24.
//

#include <doctest/doctest.h>
#include "TestUtil.h"
#include "Globals.h"

/**
 * TPCH test (single compute node, also as the single FPDB store node)
 *
 * Start Calcite server before running this
 *
 * Since the minimal size of data that we can generate is sf1 which is too large to directly put under
 * the code base, the user should prepare the `dsb-sf1/` data prior to running DSB tests
 */
namespace fpdb::main::test {

#define SKIP_SUITE false

TEST_SUITE ("dsb-sf1-parquet-spj" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("dsb-sf1-parquet-spj-13" * doctest::skip(false || SKIP_SUITE)) {
  TestUtil::startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("dsb-sf1/parquet/",
                                            {"dsb/original/spj/q13_spj.sql"},
                                            PARALLEL_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  TestUtil::stopFPDBStoreServer();
}

TEST_CASE ("dsb-sf1-parquet-spj-18" * doctest::skip(false || SKIP_SUITE)) {
  TestUtil::startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("dsb-sf1/parquet/",
                                            {"dsb/original/spj/q18_spj.sql"},
                                            PARALLEL_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  TestUtil::stopFPDBStoreServer();
}

TEST_CASE ("dsb-sf1-parquet-spj-19" * doctest::skip(false || SKIP_SUITE)) {
  TestUtil::startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("dsb-sf1/parquet/",
                                            {"dsb/original/spj/q19_spj.sql"},
                                            PARALLEL_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  TestUtil::stopFPDBStoreServer();
}

TEST_CASE ("dsb-sf1-parquet-spj-25" * doctest::skip(false || SKIP_SUITE)) {
  TestUtil::startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("dsb-sf1/parquet/",
                                            {"dsb/original/spj/q25_spj.sql"},
                                            PARALLEL_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  TestUtil::stopFPDBStoreServer();
}

TEST_CASE ("dsb-sf1-parquet-spj-27" * doctest::skip(false || SKIP_SUITE)) {
  TestUtil::startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("dsb-sf1/parquet/",
                                            {"dsb/original/spj/q27_spj.sql"},
                                            PARALLEL_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  TestUtil::stopFPDBStoreServer();
}

TEST_CASE ("dsb-sf1-parquet-spj-40" * doctest::skip(false || SKIP_SUITE)) {
  TestUtil::startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("dsb-sf1/parquet/",
                                            {"dsb/original/spj/q40_spj.sql"},
                                            PARALLEL_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  TestUtil::stopFPDBStoreServer();
}

TEST_CASE ("dsb-sf1-parquet-spj-50" * doctest::skip(false || SKIP_SUITE)) {
  TestUtil::startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("dsb-sf1/parquet/",
                                            {"dsb/original/spj/q50_spj.sql"},
                                            PARALLEL_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  TestUtil::stopFPDBStoreServer();
}

TEST_CASE ("dsb-sf1-parquet-spj-72" * doctest::skip(false || SKIP_SUITE)) {
  TestUtil::startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("dsb-sf1/parquet/",
                                            {"dsb/original/spj/q72_spj.sql"},
                                            PARALLEL_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  TestUtil::stopFPDBStoreServer();
}

TEST_CASE ("dsb-sf1-parquet-spj-84" * doctest::skip(false || SKIP_SUITE)) {
  TestUtil::startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("dsb-sf1/parquet/",
                                            {"dsb/original/spj/q84_spj.sql"},
                                            PARALLEL_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  TestUtil::stopFPDBStoreServer();
}

TEST_CASE ("dsb-sf1-parquet-spj-85" * doctest::skip(false || SKIP_SUITE)) {
  TestUtil::startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("dsb-sf1/parquet/",
                                            {"dsb/original/spj/q85_spj.sql"},
                                            PARALLEL_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  TestUtil::stopFPDBStoreServer();
}

TEST_CASE ("dsb-sf1-parquet-spj-91" * doctest::skip(false || SKIP_SUITE)) {
  TestUtil::startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("dsb-sf1/parquet/",
                                            {"dsb/original/spj/q91_spj.sql"},
                                            PARALLEL_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  TestUtil::stopFPDBStoreServer();
}

TEST_CASE ("dsb-sf1-parquet-spj-99" * doctest::skip(false || SKIP_SUITE)) {
  TestUtil::startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("dsb-sf1/parquet/",
                                            {"dsb/original/spj/q99_spj.sql"},
                                            PARALLEL_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  TestUtil::stopFPDBStoreServer();
}

TEST_CASE ("dsb-sf1-parquet-spj-100" * doctest::skip(false || SKIP_SUITE)) {
  TestUtil::startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("dsb-sf1/parquet/",
                                            {"dsb/original/spj/q100_spj.sql"},
                                            PARALLEL_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  TestUtil::stopFPDBStoreServer();
}

TEST_CASE ("dsb-sf1-parquet-spj-101" * doctest::skip(false || SKIP_SUITE)) {
  TestUtil::startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("dsb-sf1/parquet/",
                                            {"dsb/original/spj/q101_spj.sql"},
                                            PARALLEL_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pullupMode(),
                                            CachingPolicyType::NONE,
                                            1L * 1024 * 1024 * 1024,
                                            false));
  TestUtil::stopFPDBStoreServer();
}

TEST_CASE ("dsb-sf1-parquet-spj-102" * doctest::skip(false || SKIP_SUITE)) {
  TestUtil::startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("dsb-sf1/parquet/",
                                            {"dsb/modified/spj/q102_spj_nopt.sql"},
                                            PARALLEL_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  TestUtil::stopFPDBStoreServer();
}

}

TEST_SUITE ("dsb-sf1-parquet-mb" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("dsb-sf1-parquet-mb-1" * doctest::skip(false || SKIP_SUITE)) {
  TestUtil::startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("dsb-sf1/parquet/",
                                            {"dsb/original/multi-block/q1.sql"},
                                            PARALLEL_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  TestUtil::stopFPDBStoreServer();
}

TEST_CASE ("dsb-sf1-parquet-mb-10" * doctest::skip(false || SKIP_SUITE)) {
  TestUtil::startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("dsb-sf1/parquet/",
                                            {"dsb/original/multi-block/q10.sql"},
                                            PARALLEL_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  TestUtil::stopFPDBStoreServer();
}

TEST_CASE ("dsb-sf1-parquet-mb-14" * doctest::skip(false || SKIP_SUITE)) {
  TestUtil::startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("dsb-sf1/parquet/",
                                            {"dsb/original/multi-block/q14.sql"},
                                            PARALLEL_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  TestUtil::stopFPDBStoreServer();
}

TEST_CASE ("dsb-sf1-parquet-mb-23" * doctest::skip(false || SKIP_SUITE)) {
  TestUtil::startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("dsb-sf1/parquet/",
                                            {"dsb/original/multi-block/q23.sql"},
                                            PARALLEL_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  TestUtil::stopFPDBStoreServer();
}

TEST_CASE ("dsb-sf1-parquet-mb-30" * doctest::skip(false || SKIP_SUITE)) {
  TestUtil::startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("dsb-sf1/parquet/",
                                            {"dsb/original/multi-block/q30.sql"},
                                            PARALLEL_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  TestUtil::stopFPDBStoreServer();
}

TEST_CASE ("dsb-sf1-parquet-mb-31" * doctest::skip(false || SKIP_SUITE)) {
  TestUtil::startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("dsb-sf1/parquet/",
                                            {"dsb/original/multi-block/q31.sql"},
                                            PARALLEL_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  TestUtil::stopFPDBStoreServer();
}

TEST_CASE ("dsb-sf1-parquet-mb-32" * doctest::skip(false || SKIP_SUITE)) {
  TestUtil::startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("dsb-sf1/parquet/",
                                            {"dsb/original/multi-block/q32.sql"},
                                            PARALLEL_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  TestUtil::stopFPDBStoreServer();
}

TEST_CASE ("dsb-sf1-parquet-mb-38" * doctest::skip(false || SKIP_SUITE)) {
  TestUtil::startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("dsb-sf1/parquet/",
                                            {"dsb/original/multi-block/q38.sql"},
                                            PARALLEL_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  TestUtil::stopFPDBStoreServer();
}

TEST_CASE ("dsb-sf1-parquet-mb-39-0" * doctest::skip(false || SKIP_SUITE)) {
  TestUtil::startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("dsb-sf1/parquet/",
                                            {"dsb/original/multi-block/q39_0.sql"},
                                            PARALLEL_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  TestUtil::stopFPDBStoreServer();
}

TEST_CASE ("dsb-sf1-parquet-mb-39-1" * doctest::skip(false || SKIP_SUITE)) {
  TestUtil::startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("dsb-sf1/parquet/",
                                            {"dsb/original/multi-block/q39_1.sql"},
                                            PARALLEL_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  TestUtil::stopFPDBStoreServer();
}

TEST_CASE ("dsb-sf1-parquet-mb-54" * doctest::skip(false || SKIP_SUITE)) {
  TestUtil::startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("dsb-sf1/parquet/",
                                            {"dsb/original/multi-block/q54.sql"},
                                            PARALLEL_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  TestUtil::stopFPDBStoreServer();
}

TEST_CASE ("dsb-sf1-parquet-mb-58" * doctest::skip(false || SKIP_SUITE)) {
  TestUtil::startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("dsb-sf1/parquet/",
                                            {"dsb/original/multi-block/q58.sql"},
                                            PARALLEL_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  TestUtil::stopFPDBStoreServer();
}

TEST_CASE ("dsb-sf1-parquet-mb-59" * doctest::skip(false || SKIP_SUITE)) {
  TestUtil::startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("dsb-sf1/parquet/",
                                            {"dsb/original/multi-block/q59.sql"},
                                            PARALLEL_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  TestUtil::stopFPDBStoreServer();
}

TEST_CASE ("dsb-sf1-parquet-mb-64" * doctest::skip(false || SKIP_SUITE)) {
  TestUtil::startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("dsb-sf1/parquet/",
                                            {"dsb/original/multi-block/q64.sql"},
                                            PARALLEL_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  TestUtil::stopFPDBStoreServer();
}

TEST_CASE ("dsb-sf1-parquet-mb-65" * doctest::skip(false || SKIP_SUITE)) {
  TestUtil::startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("dsb-sf1/parquet/",
                                            {"dsb/original/multi-block/q65.sql"},
                                            PARALLEL_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  TestUtil::stopFPDBStoreServer();
}

TEST_CASE ("dsb-sf1-parquet-mb-69" * doctest::skip(false || SKIP_SUITE)) {
  TestUtil::startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("dsb-sf1/parquet/",
                                            {"dsb/original/multi-block/q69.sql"},
                                            PARALLEL_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  TestUtil::stopFPDBStoreServer();
}

TEST_CASE ("dsb-sf1-parquet-mb-75" * doctest::skip(false || SKIP_SUITE)) {
  TestUtil::startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("dsb-sf1/parquet/",
                                            {"dsb/original/multi-block/q75.sql"},
                                            PARALLEL_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  TestUtil::stopFPDBStoreServer();
}

TEST_CASE ("dsb-sf1-parquet-mb-80" * doctest::skip(false || SKIP_SUITE)) {
  TestUtil::startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("dsb-sf1/parquet/",
                                            {"dsb/original/multi-block/q80.sql"},
                                            PARALLEL_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  TestUtil::stopFPDBStoreServer();
}

TEST_CASE ("dsb-sf1-parquet-mb-81" * doctest::skip(false || SKIP_SUITE)) {
  TestUtil::startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("dsb-sf1/parquet/",
                                            {"dsb/original/multi-block/q81.sql"},
                                            PARALLEL_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  TestUtil::stopFPDBStoreServer();
}

TEST_CASE ("dsb-sf1-parquet-mb-83" * doctest::skip(false || SKIP_SUITE)) {
  TestUtil::startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("dsb-sf1/parquet/",
                                            {"dsb/original/multi-block/q83.sql"},
                                            PARALLEL_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  TestUtil::stopFPDBStoreServer();
}

TEST_CASE ("dsb-sf1-parquet-mb-87" * doctest::skip(false || SKIP_SUITE)) {
  TestUtil::startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("dsb-sf1/parquet/",
                                            {"dsb/original/multi-block/q87.sql"},
                                            PARALLEL_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  TestUtil::stopFPDBStoreServer();
}

TEST_CASE ("dsb-sf1-parquet-mb-92" * doctest::skip(false || SKIP_SUITE)) {
  TestUtil::startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("dsb-sf1/parquet/",
                                            {"dsb/original/multi-block/q92.sql"},
                                            PARALLEL_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  TestUtil::stopFPDBStoreServer();
}

TEST_CASE ("dsb-sf1-parquet-mb-94" * doctest::skip(false || SKIP_SUITE)) {
  TestUtil::startFPDBStoreServer();
  REQUIRE(TestUtil::e2eNoStartCalciteServer("dsb-sf1/parquet/",
                                            {"dsb/original/multi-block/q94.sql"},
                                            PARALLEL_FPDB_STORE_SAME_NODE,
                                            false,
                                            ObjStoreType::FPDB_STORE));
  TestUtil::stopFPDBStoreServer();
}

}

}
