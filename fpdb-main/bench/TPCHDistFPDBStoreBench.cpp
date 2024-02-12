//
// Created by Yifei Yang on 9/12/22.
//

#include <doctest/doctest.h>
#include "TestUtil.h"
#include "Globals.h"

/**
 * TPCH test (multiple compute nodes, FPDB store)
 *
 * Start Calcite server on the coordinator (locally), CAF server on all executors (remote nodes),
 * and FPDB store server before running this (see README for more details)
 */
namespace fpdb::main::test {

#define SKIP_SUITE false

TEST_SUITE ("tpch-sf10-fpdb-store-distributed-csv-pullup" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("tpch-sf10-fpdb-store-distributed-csv-pullup-01" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/01.sql"},
                                            PARALLEL_DIST_SF10,
                                            true,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("tpch-sf10-fpdb-store-distributed-csv-pullup-02" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/02.sql"},
                                            PARALLEL_DIST_SF10,
                                            true,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("tpch-sf10-fpdb-store-distributed-csv-pullup-03" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/03.sql"},
                                            PARALLEL_DIST_SF10,
                                            true,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("tpch-sf10-fpdb-store-distributed-csv-pullup-04" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/04.sql"},
                                            PARALLEL_DIST_SF10,
                                            true,
                                            ObjStoreType::FPDB_STORE));
}

// FIXME: not considering key-foreign key constraint with filtering leads to a bad query plan
//  currently manually specify the join order
TEST_CASE ("tpch-sf10-fpdb-store-distributed-csv-pullup-05" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServerNoHeuristicJoinOrdering("tpch-sf10/csv/",
                                                                   {"tpch/original/05.sql"},
                                                                   PARALLEL_DIST_SF10,
                                                                   true,
                                                                   ObjStoreType::FPDB_STORE));
}

TEST_CASE ("tpch-sf10-fpdb-store-distributed-csv-pullup-06" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/06.sql"},
                                            PARALLEL_DIST_SF10,
                                            true,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("tpch-sf10-fpdb-store-distributed-csv-pullup-07" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/07.sql"},
                                            PARALLEL_DIST_SF10,
                                            true,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("tpch-sf10-fpdb-store-distributed-csv-pullup-08" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/08.sql"},
                                            PARALLEL_DIST_SF10,
                                            true,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("tpch-sf10-fpdb-store-distributed-csv-pullup-09" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/09.sql"},
                                            PARALLEL_DIST_SF10,
                                            true,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("tpch-sf10-fpdb-store-distributed-csv-pullup-10" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/10.sql"},
                                            PARALLEL_DIST_SF10,
                                            true,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("tpch-sf10-fpdb-store-distributed-csv-pullup-11" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/11.sql"},
                                            PARALLEL_DIST_SF10,
                                            true,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("tpch-sf10-fpdb-store-distributed-csv-pullup-12" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/12.sql"},
                                            PARALLEL_DIST_SF10,
                                            true,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("tpch-sf10-fpdb-store-distributed-csv-pullup-13" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/13.sql"},
                                            PARALLEL_DIST_SF10,
                                            true,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("tpch-sf10-fpdb-store-distributed-csv-pullup-14" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/14.sql"},
                                            PARALLEL_DIST_SF10,
                                            true,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("tpch-sf10-fpdb-store-distributed-csv-pullup-15" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/15.sql"},
                                            PARALLEL_DIST_SF10,
                                            true,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("tpch-sf10-fpdb-store-distributed-csv-pullup-16" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/16.sql"},
                                            PARALLEL_DIST_SF10,
                                            true,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("tpch-sf10-fpdb-store-distributed-csv-pullup-17" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/17.sql"},
                                            PARALLEL_DIST_SF10,
                                            true,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("tpch-sf10-fpdb-store-distributed-csv-pullup-18" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/18.sql"},
                                            PARALLEL_DIST_SF10,
                                            true,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("tpch-sf10-fpdb-store-distributed-csv-pullup-19" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/19.sql"},
                                            PARALLEL_DIST_SF10,
                                            true,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("tpch-sf10-fpdb-store-distributed-csv-pullup-20" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/20.sql"},
                                            PARALLEL_DIST_SF10,
                                            true,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("tpch-sf10-fpdb-store-distributed-csv-pullup-21" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/21.sql"},
                                            PARALLEL_DIST_SF10,
                                            true,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("tpch-sf10-fpdb-store-distributed-csv-pullup-22" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/22.sql"},
                                            PARALLEL_DIST_SF10,
                                            true,
                                            ObjStoreType::FPDB_STORE));
}

}

TEST_SUITE ("tpch-sf10-fpdb-store-distributed-parquet-pullup" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("tpch-sf10-fpdb-store-distributed-parquet-pullup-01" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/parquet/",
                                            {"tpch/original/01.sql"},
                                            PARALLEL_DIST_SF10,
                                            true,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("tpch-sf10-fpdb-store-distributed-parquet-pullup-02" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/parquet/",
                                            {"tpch/original/02.sql"},
                                            PARALLEL_DIST_SF10,
                                            true,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("tpch-sf10-fpdb-store-distributed-parquet-pullup-03" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/parquet/",
                                            {"tpch/original/03.sql"},
                                            PARALLEL_DIST_SF10,
                                            true,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("tpch-sf10-fpdb-store-distributed-parquet-pullup-04" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/parquet/",
                                            {"tpch/original/04.sql"},
                                            PARALLEL_DIST_SF10,
                                            true,
                                            ObjStoreType::FPDB_STORE));
}

// FIXME: not considering key-foreign key constraint with filtering leads to a bad query plan
//  currently manually specify the join order
TEST_CASE ("tpch-sf10-fpdb-store-distributed-parquet-pullup-05" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServerNoHeuristicJoinOrdering("tpch-sf10/parquet/",
                                                                   {"tpch/original/05.sql"},
                                                                   PARALLEL_DIST_SF10,
                                                                   true,
                                                                   ObjStoreType::FPDB_STORE));
}

TEST_CASE ("tpch-sf10-fpdb-store-distributed-parquet-pullup-06" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/parquet/",
                                            {"tpch/original/06.sql"},
                                            PARALLEL_DIST_SF10,
                                            true,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("tpch-sf10-fpdb-store-distributed-parquet-pullup-07" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/parquet/",
                                            {"tpch/original/07.sql"},
                                            PARALLEL_DIST_SF10,
                                            true,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("tpch-sf10-fpdb-store-distributed-parquet-pullup-08" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/parquet/",
                                            {"tpch/original/08.sql"},
                                            PARALLEL_DIST_SF10,
                                            true,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("tpch-sf10-fpdb-store-distributed-parquet-pullup-09" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/parquet/",
                                            {"tpch/original/09.sql"},
                                            PARALLEL_DIST_SF10,
                                            true,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("tpch-sf10-fpdb-store-distributed-parquet-pullup-10" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/parquet/",
                                            {"tpch/original/10.sql"},
                                            PARALLEL_DIST_SF10,
                                            true,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("tpch-sf10-fpdb-store-distributed-parquet-pullup-11" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/parquet/",
                                            {"tpch/original/11.sql"},
                                            PARALLEL_DIST_SF10,
                                            true,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("tpch-sf10-fpdb-store-distributed-parquet-pullup-12" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/parquet/",
                                            {"tpch/original/12.sql"},
                                            PARALLEL_DIST_SF10,
                                            true,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("tpch-sf10-fpdb-store-distributed-parquet-pullup-13" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/parquet/",
                                            {"tpch/original/13.sql"},
                                            PARALLEL_DIST_SF10,
                                            true,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("tpch-sf10-fpdb-store-distributed-parquet-pullup-14" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/parquet/",
                                            {"tpch/original/14.sql"},
                                            PARALLEL_DIST_SF10,
                                            true,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("tpch-sf10-fpdb-store-distributed-parquet-pullup-15" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/parquet/",
                                            {"tpch/original/15.sql"},
                                            PARALLEL_DIST_SF10,
                                            true,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("tpch-sf10-fpdb-store-distributed-parquet-pullup-16" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/parquet/",
                                            {"tpch/original/16.sql"},
                                            PARALLEL_DIST_SF10,
                                            true,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("tpch-sf10-fpdb-store-distributed-parquet-pullup-17" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/parquet/",
                                            {"tpch/original/17.sql"},
                                            PARALLEL_DIST_SF10,
                                            true,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("tpch-sf10-fpdb-store-distributed-parquet-pullup-18" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/parquet/",
                                            {"tpch/original/18.sql"},
                                            PARALLEL_DIST_SF10,
                                            true,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("tpch-sf10-fpdb-store-distributed-parquet-pullup-19" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/parquet/",
                                            {"tpch/original/19.sql"},
                                            PARALLEL_DIST_SF10,
                                            true,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("tpch-sf10-fpdb-store-distributed-parquet-pullup-20" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/parquet/",
                                            {"tpch/original/20.sql"},
                                            PARALLEL_DIST_SF10,
                                            true,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("tpch-sf10-fpdb-store-distributed-parquet-pullup-21" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/parquet/",
                                            {"tpch/original/21.sql"},
                                            PARALLEL_DIST_SF10,
                                            true,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("tpch-sf10-fpdb-store-distributed-parquet-pullup-22" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/parquet/",
                                            {"tpch/original/22.sql"},
                                            PARALLEL_DIST_SF10,
                                            true,
                                            ObjStoreType::FPDB_STORE));
}

}

TEST_SUITE ("tpch-sf10-fpdb-store-distributed-csv-pushdown-only" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("tpch-sf10-fpdb-store-distributed-csv-pushdown-only-01" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/01.sql"},
                                            PARALLEL_DIST_SF10,
                                            true,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("tpch-sf10-fpdb-store-distributed-csv-pushdown-only-02" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/02.sql"},
                                            PARALLEL_DIST_SF10,
                                            true,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("tpch-sf10-fpdb-store-distributed-csv-pushdown-only-03" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/03.sql"},
                                            PARALLEL_DIST_SF10,
                                            true,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("tpch-sf10-fpdb-store-distributed-csv-pushdown-only-04" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/04.sql"},
                                            PARALLEL_DIST_SF10,
                                            true,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

// FIXME: not considering key-foreign key constraint with filtering leads to a bad query plan
//  currently manually specify the join order
TEST_CASE ("tpch-sf10-fpdb-store-distributed-csv-pushdown-only-05" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServerNoHeuristicJoinOrdering("tpch-sf10/csv/",
                                                                   {"tpch/original/05.sql"},
                                                                   PARALLEL_DIST_SF10,
                                                                   true,
                                                                   ObjStoreType::FPDB_STORE,
                                                                   Mode::pushdownOnlyMode()));
}

TEST_CASE ("tpch-sf10-fpdb-store-distributed-csv-pushdown-only-06" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/06.sql"},
                                            PARALLEL_DIST_SF10,
                                            true,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("tpch-sf10-fpdb-store-distributed-csv-pushdown-only-07" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/07.sql"},
                                            PARALLEL_DIST_SF10,
                                            true,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("tpch-sf10-fpdb-store-distributed-csv-pushdown-only-08" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/08.sql"},
                                            PARALLEL_DIST_SF10,
                                            true,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("tpch-sf10-fpdb-store-distributed-csv-pushdown-only-09" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/09.sql"},
                                            PARALLEL_DIST_SF10,
                                            true,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("tpch-sf10-fpdb-store-distributed-csv-pushdown-only-10" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/10.sql"},
                                            PARALLEL_DIST_SF10,
                                            true,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("tpch-sf10-fpdb-store-distributed-csv-pushdown-only-11" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/11.sql"},
                                            PARALLEL_DIST_SF10,
                                            true,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("tpch-sf10-fpdb-store-distributed-csv-pushdown-only-12" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/12.sql"},
                                            PARALLEL_DIST_SF10,
                                            true,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("tpch-sf10-fpdb-store-distributed-csv-pushdown-only-13" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/13.sql"},
                                            PARALLEL_DIST_SF10,
                                            true,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("tpch-sf10-fpdb-store-distributed-csv-pushdown-only-14" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/14.sql"},
                                            PARALLEL_DIST_SF10,
                                            true,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("tpch-sf10-fpdb-store-distributed-csv-pushdown-only-15" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/15.sql"},
                                            PARALLEL_DIST_SF10,
                                            true,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("tpch-sf10-fpdb-store-distributed-csv-pushdown-only-16" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/16.sql"},
                                            PARALLEL_DIST_SF10,
                                            true,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("tpch-sf10-fpdb-store-distributed-csv-pushdown-only-17" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/17.sql"},
                                            PARALLEL_DIST_SF10,
                                            true,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("tpch-sf10-fpdb-store-distributed-csv-pushdown-only-18" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/18.sql"},
                                            PARALLEL_DIST_SF10,
                                            true,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("tpch-sf10-fpdb-store-distributed-csv-pushdown-only-19" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/19.sql"},
                                            PARALLEL_DIST_SF10,
                                            true,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("tpch-sf10-fpdb-store-distributed-csv-pushdown-only-20" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/20.sql"},
                                            PARALLEL_DIST_SF10,
                                            true,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("tpch-sf10-fpdb-store-distributed-csv-pushdown-only-21" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/21.sql"},
                                            PARALLEL_DIST_SF10,
                                            true,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("tpch-sf10-fpdb-store-distributed-csv-pushdown-only-22" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/22.sql"},
                                            PARALLEL_DIST_SF10,
                                            true,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

}

TEST_SUITE ("tpch-sf10-fpdb-store-distributed-parquet-pushdown-only" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("tpch-sf10-fpdb-store-distributed-parquet-pushdown-only-01" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/parquet/",
                                            {"tpch/original/01.sql"},
                                            PARALLEL_DIST_SF10,
                                            true,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("tpch-sf10-fpdb-store-distributed-parquet-pushdown-only-02" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/parquet/",
                                            {"tpch/original/02.sql"},
                                            PARALLEL_DIST_SF10,
                                            true,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("tpch-sf10-fpdb-store-distributed-parquet-pushdown-only-03" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/parquet/",
                                            {"tpch/original/03.sql"},
                                            PARALLEL_DIST_SF10,
                                            true,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("tpch-sf10-fpdb-store-distributed-parquet-pushdown-only-04" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/parquet/",
                                            {"tpch/original/04.sql"},
                                            PARALLEL_DIST_SF10,
                                            true,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

// FIXME: not considering key-foreign key constraint with filtering leads to a bad query plan
//  currently manually specify the join order
TEST_CASE ("tpch-sf10-fpdb-store-distributed-parquet-pushdown-only-05" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServerNoHeuristicJoinOrdering("tpch-sf10/parquet/",
                                                                   {"tpch/original/05.sql"},
                                                                   PARALLEL_DIST_SF10,
                                                                   true,
                                                                   ObjStoreType::FPDB_STORE,
                                                                   Mode::pushdownOnlyMode()));
}

TEST_CASE ("tpch-sf10-fpdb-store-distributed-parquet-pushdown-only-06" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/parquet/",
                                            {"tpch/original/06.sql"},
                                            PARALLEL_DIST_SF10,
                                            true,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("tpch-sf10-fpdb-store-distributed-parquet-pushdown-only-07" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/parquet/",
                                            {"tpch/original/07.sql"},
                                            PARALLEL_DIST_SF10,
                                            true,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("tpch-sf10-fpdb-store-distributed-parquet-pushdown-only-08" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/parquet/",
                                            {"tpch/original/08.sql"},
                                            PARALLEL_DIST_SF10,
                                            true,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("tpch-sf10-fpdb-store-distributed-parquet-pushdown-only-09" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/parquet/",
                                            {"tpch/original/09.sql"},
                                            PARALLEL_DIST_SF10,
                                            true,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("tpch-sf10-fpdb-store-distributed-parquet-pushdown-only-10" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/parquet/",
                                            {"tpch/original/10.sql"},
                                            PARALLEL_DIST_SF10,
                                            true,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("tpch-sf10-fpdb-store-distributed-parquet-pushdown-only-11" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/parquet/",
                                            {"tpch/original/11.sql"},
                                            PARALLEL_DIST_SF10,
                                            true,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("tpch-sf10-fpdb-store-distributed-parquet-pushdown-only-12" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/parquet/",
                                            {"tpch/original/12.sql"},
                                            PARALLEL_DIST_SF10,
                                            true,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("tpch-sf10-fpdb-store-distributed-parquet-pushdown-only-13" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/parquet/",
                                            {"tpch/original/13.sql"},
                                            PARALLEL_DIST_SF10,
                                            true,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("tpch-sf10-fpdb-store-distributed-parquet-pushdown-only-14" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/parquet/",
                                            {"tpch/original/14.sql"},
                                            PARALLEL_DIST_SF10,
                                            true,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("tpch-sf10-fpdb-store-distributed-parquet-pushdown-only-15" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/parquet/",
                                            {"tpch/original/15.sql"},
                                            PARALLEL_DIST_SF10,
                                            true,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("tpch-sf10-fpdb-store-distributed-parquet-pushdown-only-16" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/parquet/",
                                            {"tpch/original/16.sql"},
                                            PARALLEL_DIST_SF10,
                                            true,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("tpch-sf10-fpdb-store-distributed-parquet-pushdown-only-17" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/parquet/",
                                            {"tpch/original/17.sql"},
                                            PARALLEL_DIST_SF10,
                                            true,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("tpch-sf10-fpdb-store-distributed-parquet-pushdown-only-18" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/parquet/",
                                            {"tpch/original/18.sql"},
                                            PARALLEL_DIST_SF10,
                                            true,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("tpch-sf10-fpdb-store-distributed-parquet-pushdown-only-19" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/parquet/",
                                            {"tpch/original/19.sql"},
                                            PARALLEL_DIST_SF10,
                                            true,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("tpch-sf10-fpdb-store-distributed-parquet-pushdown-only-20" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/parquet/",
                                            {"tpch/original/20.sql"},
                                            PARALLEL_DIST_SF10,
                                            true,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("tpch-sf10-fpdb-store-distributed-parquet-pushdown-only-21" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/parquet/",
                                            {"tpch/original/21.sql"},
                                            PARALLEL_DIST_SF10,
                                            true,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("tpch-sf10-fpdb-store-distributed-parquet-pushdown-only-22" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/parquet/",
                                            {"tpch/original/22.sql"},
                                            PARALLEL_DIST_SF10,
                                            true,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

}

TEST_SUITE ("tpch-sf10-2-node-hash-part-hash-join-pushdown" * doctest::skip(SKIP_SUITE)) {

// Enable co-located hash join pushdown first
// for 'tpch-sf10-2-node-hash-part', 'lineitem' and 'orders' are co-located at 'l_orderkey = o.o_orderkey'
TEST_CASE ("tpch-sf10-2-node-hash-part-hash-join-pushable-04" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10-2-node-hash-part/parquet/",
                                            {"tpch/original/04.sql"},
                                            PARALLEL_DIST_SF10,
                                            true,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

// pullup baseline for "tpch-sf10-2-node-hash-part-hash-join-pushable-04"
TEST_CASE ("tpch-sf10-2-node-hash-part-hash-join-pullup-04" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10-2-node-hash-part/parquet/",
                                            {"tpch/original/04.sql"},
                                            PARALLEL_DIST_SF10,
                                            true,
                                            ObjStoreType::FPDB_STORE));
}

// Enable co-located hash join pushdown first
// for 'tpch-sf10-2-node-hash-part', 'lineitem' and 'orders' are co-located at 'l_orderkey = o.o_orderkey'
TEST_CASE ("tpch-sf10-2-node-hash-part-hash-join-synthetic-2-table" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10-2-node-hash-part/parquet/",
                                            {"tpch/synthetic/co-join-2-table.sql"},
                                            PARALLEL_DIST_SF10,
                                            true,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

// pullup baseline for "tpch-sf10-2-node-hash-part-hash-join-synthetic-2-table-pullup"
TEST_CASE ("tpch-sf10-2-node-hash-part-hash-join-synthetic-2-table-pullup" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10-2-node-hash-part/parquet/",
                                            {"tpch/synthetic/co-join-2-table.sql"},
                                            PARALLEL_DIST_SF10,
                                            true,
                                            ObjStoreType::FPDB_STORE));
}

// Enable co-located hash join pushdown first
// for 'tpch-sf10-2-node-hash-part', 'lineitem' and 'orders' are co-located at 'l_orderkey = o.o_orderkey'
TEST_CASE ("tpch-sf10-2-node-hash-part-hash-join-synthetic-3-table" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10-2-node-hash-part/parquet/",
                                            {"tpch/synthetic/co-join-3-table.sql"},
                                            PARALLEL_DIST_SF10,
                                            true,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

// pullup baseline for "tpch-sf10-2-node-hash-part-hash-join-synthetic-3-table-pullup"
TEST_CASE ("tpch-sf10-2-node-hash-part-hash-join-synthetic-3-table-pullup" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10-2-node-hash-part/parquet/",
                                            {"tpch/synthetic/co-join-3-table.sql"},
                                            PARALLEL_DIST_SF10,
                                            true,
                                            ObjStoreType::FPDB_STORE));
}

}

TEST_SUITE ("tpch-sf100-4-node-hash-part-fpdb-store-distributed-parquet-pullup" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("tpch-sf100-4-node-hash-part-fpdb-store-distributed-parquet-pullup-01" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf100-4-node-hash-part/parquet/",
                                            {"tpch/original/01.sql"},
                                            PARALLEL_DIST_SF100,
                                            true,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("tpch-sf100-4-node-hash-part-fpdb-store-distributed-parquet-pullup-02" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf100-4-node-hash-part/parquet/",
                                            {"tpch/original/02.sql"},
                                            PARALLEL_DIST_SF100,
                                            true,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("tpch-sf100-4-node-hash-part-fpdb-store-distributed-parquet-pullup-03" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf100-4-node-hash-part/parquet/",
                                            {"tpch/original/03.sql"},
                                            PARALLEL_DIST_SF100,
                                            true,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("tpch-sf100-4-node-hash-part-fpdb-store-distributed-parquet-pullup-04" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf100-4-node-hash-part/parquet/",
                                            {"tpch/original/04.sql"},
                                            PARALLEL_DIST_SF100,
                                            true,
                                            ObjStoreType::FPDB_STORE));
}

// FIXME: not considering key-foreign key constraint with filtering leads to a bad query plan
//  currently manually specify the join order
TEST_CASE ("tpch-sf100-4-node-hash-part-fpdb-store-distributed-parquet-pullup-05" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServerNoHeuristicJoinOrdering("tpch-sf100-4-node-hash-part/parquet/",
                                                                   {"tpch/original/05.sql"},
                                                                   PARALLEL_DIST_SF100,
                                                                   true,
                                                                   ObjStoreType::FPDB_STORE));
}

TEST_CASE ("tpch-sf100-4-node-hash-part-fpdb-store-distributed-parquet-pullup-06" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf100-4-node-hash-part/parquet/",
                                            {"tpch/original/06.sql"},
                                            PARALLEL_DIST_SF100,
                                            true,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("tpch-sf100-4-node-hash-part-fpdb-store-distributed-parquet-pullup-07" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf100-4-node-hash-part/parquet/",
                                            {"tpch/original/07.sql"},
                                            PARALLEL_DIST_SF100,
                                            true,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("tpch-sf100-4-node-hash-part-fpdb-store-distributed-parquet-pullup-08" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf100-4-node-hash-part/parquet/",
                                            {"tpch/original/08.sql"},
                                            PARALLEL_DIST_SF100,
                                            true,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("tpch-sf100-4-node-hash-part-fpdb-store-distributed-parquet-pullup-09" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf100-4-node-hash-part/parquet/",
                                            {"tpch/original/09.sql"},
                                            PARALLEL_DIST_SF100,
                                            true,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("tpch-sf100-4-node-hash-part-fpdb-store-distributed-parquet-pullup-10" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf100-4-node-hash-part/parquet/",
                                            {"tpch/original/10.sql"},
                                            PARALLEL_DIST_SF100,
                                            true,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("tpch-sf100-4-node-hash-part-fpdb-store-distributed-parquet-pullup-11" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf100-4-node-hash-part/parquet/",
                                            {"tpch/original/11.sql"},
                                            PARALLEL_DIST_SF100,
                                            true,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("tpch-sf100-4-node-hash-part-fpdb-store-distributed-parquet-pullup-12" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf100-4-node-hash-part/parquet/",
                                            {"tpch/original/12.sql"},
                                            PARALLEL_DIST_SF100,
                                            true,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("tpch-sf100-4-node-hash-part-fpdb-store-distributed-parquet-pullup-13" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf100-4-node-hash-part/parquet/",
                                            {"tpch/original/13.sql"},
                                            PARALLEL_DIST_SF100,
                                            true,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("tpch-sf100-4-node-hash-part-fpdb-store-distributed-parquet-pullup-14" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf100-4-node-hash-part/parquet/",
                                            {"tpch/original/14.sql"},
                                            PARALLEL_DIST_SF100,
                                            true,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("tpch-sf100-4-node-hash-part-fpdb-store-distributed-parquet-pullup-15" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf100-4-node-hash-part/parquet/",
                                            {"tpch/original/15.sql"},
                                            PARALLEL_DIST_SF100,
                                            true,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("tpch-sf100-4-node-hash-part-fpdb-store-distributed-parquet-pullup-16" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf100-4-node-hash-part/parquet/",
                                            {"tpch/original/16.sql"},
                                            PARALLEL_DIST_SF100,
                                            true,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("tpch-sf100-4-node-hash-part-fpdb-store-distributed-parquet-pullup-17" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf100-4-node-hash-part/parquet/",
                                            {"tpch/original/17.sql"},
                                            PARALLEL_DIST_SF100,
                                            true,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("tpch-sf100-4-node-hash-part-fpdb-store-distributed-parquet-pullup-18" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf100-4-node-hash-part/parquet/",
                                            {"tpch/original/18.sql"},
                                            PARALLEL_DIST_SF100,
                                            true,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("tpch-sf100-4-node-hash-part-fpdb-store-distributed-parquet-pullup-19" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf100-4-node-hash-part/parquet/",
                                            {"tpch/original/19.sql"},
                                            PARALLEL_DIST_SF100,
                                            true,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("tpch-sf100-4-node-hash-part-fpdb-store-distributed-parquet-pullup-20" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf100-4-node-hash-part/parquet/",
                                            {"tpch/original/20.sql"},
                                            PARALLEL_DIST_SF100,
                                            true,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("tpch-sf100-4-node-hash-part-fpdb-store-distributed-parquet-pullup-21" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf100-4-node-hash-part/parquet/",
                                            {"tpch/original/21.sql"},
                                            PARALLEL_DIST_SF100,
                                            true,
                                            ObjStoreType::FPDB_STORE));
}

TEST_CASE ("tpch-sf100-4-node-hash-part-fpdb-store-distributed-parquet-pullup-22" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf100-4-node-hash-part/parquet/",
                                            {"tpch/original/22.sql"},
                                            PARALLEL_DIST_SF100,
                                            true,
                                            ObjStoreType::FPDB_STORE));
}

}

TEST_SUITE ("tpch-sf100-4-node-hash-part-fpdb-store-distributed-parquet-pushdown-only" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("tpch-sf100-4-node-hash-part-fpdb-store-distributed-parquet-pushdown-only-01" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf100-4-node-hash-part/parquet/",
                                            {"tpch/original/01.sql"},
                                            PARALLEL_DIST_SF100,
                                            true,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("tpch-sf100-4-node-hash-part-fpdb-store-distributed-parquet-pushdown-only-02" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf100-4-node-hash-part/parquet/",
                                            {"tpch/original/02.sql"},
                                            PARALLEL_DIST_SF100,
                                            true,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("tpch-sf100-4-node-hash-part-fpdb-store-distributed-parquet-pushdown-only-03" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf100-4-node-hash-part/parquet/",
                                            {"tpch/original/03.sql"},
                                            PARALLEL_DIST_SF100,
                                            true,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("tpch-sf100-4-node-hash-part-fpdb-store-distributed-parquet-pushdown-only-04" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf100-4-node-hash-part/parquet/",
                                            {"tpch/original/04.sql"},
                                            PARALLEL_DIST_SF100,
                                            true,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

// FIXME: not considering key-foreign key constraint with filtering leads to a bad query plan
//  currently manually specify the join order
TEST_CASE ("tpch-sf100-4-node-hash-part-fpdb-store-distributed-parquet-pushdown-only-05" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServerNoHeuristicJoinOrdering("tpch-sf100-4-node-hash-part/parquet/",
                                                                   {"tpch/original/05.sql"},
                                                                   PARALLEL_DIST_SF100,
                                                                   true,
                                                                   ObjStoreType::FPDB_STORE,
                                                                   Mode::pushdownOnlyMode()));
}

TEST_CASE ("tpch-sf100-4-node-hash-part-fpdb-store-distributed-parquet-pushdown-only-06" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf100-4-node-hash-part/parquet/",
                                            {"tpch/original/06.sql"},
                                            PARALLEL_DIST_SF100,
                                            true,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("tpch-sf100-4-node-hash-part-fpdb-store-distributed-parquet-pushdown-only-07" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf100-4-node-hash-part/parquet/",
                                            {"tpch/original/07.sql"},
                                            PARALLEL_DIST_SF100,
                                            true,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("tpch-sf100-4-node-hash-part-fpdb-store-distributed-parquet-pushdown-only-08" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf100-4-node-hash-part/parquet/",
                                            {"tpch/original/08.sql"},
                                            PARALLEL_DIST_SF100,
                                            true,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("tpch-sf100-4-node-hash-part-fpdb-store-distributed-parquet-pushdown-only-09" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf100-4-node-hash-part/parquet/",
                                            {"tpch/original/09.sql"},
                                            PARALLEL_DIST_SF100,
                                            true,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("tpch-sf100-4-node-hash-part-fpdb-store-distributed-parquet-pushdown-only-10" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf100-4-node-hash-part/parquet/",
                                            {"tpch/original/10.sql"},
                                            PARALLEL_DIST_SF100,
                                            true,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("tpch-sf100-4-node-hash-part-fpdb-store-distributed-parquet-pushdown-only-11" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf100-4-node-hash-part/parquet/",
                                            {"tpch/original/11.sql"},
                                            PARALLEL_DIST_SF100,
                                            true,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("tpch-sf100-4-node-hash-part-fpdb-store-distributed-parquet-pushdown-only-12" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf100-4-node-hash-part/parquet/",
                                            {"tpch/original/12.sql"},
                                            PARALLEL_DIST_SF100,
                                            true,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("tpch-sf100-4-node-hash-part-fpdb-store-distributed-parquet-pushdown-only-13" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf100-4-node-hash-part/parquet/",
                                            {"tpch/original/13.sql"},
                                            PARALLEL_DIST_SF100,
                                            true,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("tpch-sf100-4-node-hash-part-fpdb-store-distributed-parquet-pushdown-only-14" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf100-4-node-hash-part/parquet/",
                                            {"tpch/original/14.sql"},
                                            PARALLEL_DIST_SF100,
                                            true,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("tpch-sf100-4-node-hash-part-fpdb-store-distributed-parquet-pushdown-only-15" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf100-4-node-hash-part/parquet/",
                                            {"tpch/original/15.sql"},
                                            PARALLEL_DIST_SF100,
                                            true,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("tpch-sf100-4-node-hash-part-fpdb-store-distributed-parquet-pushdown-only-16" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf100-4-node-hash-part/parquet/",
                                            {"tpch/original/16.sql"},
                                            PARALLEL_DIST_SF100,
                                            true,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("tpch-sf100-4-node-hash-part-fpdb-store-distributed-parquet-pushdown-only-17" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf100-4-node-hash-part/parquet/",
                                            {"tpch/original/17.sql"},
                                            PARALLEL_DIST_SF100,
                                            true,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("tpch-sf100-4-node-hash-part-fpdb-store-distributed-parquet-pushdown-only-18" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf100-4-node-hash-part/parquet/",
                                            {"tpch/original/18.sql"},
                                            PARALLEL_DIST_SF100,
                                            true,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("tpch-sf100-4-node-hash-part-fpdb-store-distributed-parquet-pushdown-only-19" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf100-4-node-hash-part/parquet/",
                                            {"tpch/original/19.sql"},
                                            PARALLEL_DIST_SF100,
                                            true,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("tpch-sf100-4-node-hash-part-fpdb-store-distributed-parquet-pushdown-only-20" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf100-4-node-hash-part/parquet/",
                                            {"tpch/original/20.sql"},
                                            PARALLEL_DIST_SF100,
                                            true,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("tpch-sf100-4-node-hash-part-fpdb-store-distributed-parquet-pushdown-only-21" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf100-4-node-hash-part/parquet/",
                                            {"tpch/original/21.sql"},
                                            PARALLEL_DIST_SF100,
                                            true,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

TEST_CASE ("tpch-sf100-4-node-hash-part-fpdb-store-distributed-parquet-pushdown-only-22" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf100-4-node-hash-part/parquet/",
                                            {"tpch/original/22.sql"},
                                            PARALLEL_DIST_SF100,
                                            true,
                                            ObjStoreType::FPDB_STORE,
                                            Mode::pushdownOnlyMode()));
}

}

}
