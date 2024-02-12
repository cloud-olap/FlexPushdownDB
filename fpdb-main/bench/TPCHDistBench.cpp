//
// Created by Yifei Yang on 4/17/22.
//

#include <doctest/doctest.h>
#include "TestUtil.h"
#include "Globals.h"

/**
 * TPCH test (multiple compute node, S3)
 *
 * Start Calcite server on the coordinator (locally) and CAF server on all executors (remote nodes) before running this
 * (see README for more details)
 */
namespace fpdb::main::test {

#define SKIP_SUITE false

TEST_SUITE ("tpch-sf10-distributed" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("tpch-sf10-distributed-01" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/01.sql"},
                                            PARALLEL_DIST_SF10,
                                            true,
                                            ObjStoreType::S3));
}

TEST_CASE ("tpch-sf10-distributed-02" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/02.sql"},
                                            PARALLEL_DIST_SF10,
                                            true,
                                            ObjStoreType::S3));
}

TEST_CASE ("tpch-sf10-distributed-03" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/03.sql"},
                                            PARALLEL_DIST_SF10,
                                            true,
                                            ObjStoreType::S3));
}

TEST_CASE ("tpch-sf10-distributed-04" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/04.sql"},
                                            PARALLEL_DIST_SF10,
                                            true,
                                            ObjStoreType::S3));
}

TEST_CASE ("tpch-sf10-distributed-05" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/05.sql"},
                                            PARALLEL_DIST_SF10,
                                            true,
                                            ObjStoreType::S3));
}

TEST_CASE ("tpch-sf10-distributed-06" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/06.sql"},
                                            PARALLEL_DIST_SF10,
                                            true,
                                            ObjStoreType::S3));
}

TEST_CASE ("tpch-sf10-distributed-07" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/07.sql"},
                                            PARALLEL_DIST_SF10,
                                            true,
                                            ObjStoreType::S3));
}

TEST_CASE ("tpch-sf10-distributed-08" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/08.sql"},
                                            PARALLEL_DIST_SF10,
                                            true,
                                            ObjStoreType::S3));
}

TEST_CASE ("tpch-sf10-distributed-09" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/09.sql"},
                                            PARALLEL_DIST_SF10,
                                            true,
                                            ObjStoreType::S3));
}

TEST_CASE ("tpch-sf10-distributed-10" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/10.sql"},
                                            PARALLEL_DIST_SF10,
                                            true,
                                            ObjStoreType::S3));
}

TEST_CASE ("tpch-sf10-distributed-11" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/11.sql"},
                                            PARALLEL_DIST_SF10,
                                            true,
                                            ObjStoreType::S3));
}

TEST_CASE ("tpch-sf10-distributed-12" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/12.sql"},
                                            PARALLEL_DIST_SF10,
                                            true,
                                            ObjStoreType::S3));
}

TEST_CASE ("tpch-sf10-distributed-13" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/13.sql"},
                                            PARALLEL_DIST_SF10,
                                            true,
                                            ObjStoreType::S3));
}

TEST_CASE ("tpch-sf10-distributed-14" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/14.sql"},
                                            PARALLEL_DIST_SF10,
                                            true,
                                            ObjStoreType::S3));
}

TEST_CASE ("tpch-sf10-distributed-15" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/15.sql"},
                                            PARALLEL_DIST_SF10,
                                            true,
                                            ObjStoreType::S3));
}

TEST_CASE ("tpch-sf10-distributed-16" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/16.sql"},
                                            PARALLEL_DIST_SF10,
                                            true,
                                            ObjStoreType::S3));
}

TEST_CASE ("tpch-sf10-distributed-17" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/17.sql"},
                                            PARALLEL_DIST_SF10,
                                            true,
                                            ObjStoreType::S3));
}

TEST_CASE ("tpch-sf10-distributed-18" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/18.sql"},
                                            PARALLEL_DIST_SF10,
                                            true,
                                            ObjStoreType::S3));
}

TEST_CASE ("tpch-sf10-distributed-19" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/19.sql"},
                                            PARALLEL_DIST_SF10,
                                            true,
                                            ObjStoreType::S3));
}

TEST_CASE ("tpch-sf10-distributed-20" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/20.sql"},
                                            PARALLEL_DIST_SF10,
                                            true,
                                            ObjStoreType::S3));
}

TEST_CASE ("tpch-sf10-distributed-21" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/21.sql"},
                                            PARALLEL_DIST_SF10,
                                            true,
                                            ObjStoreType::S3));
}

TEST_CASE ("tpch-sf10-distributed-22" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/22.sql"},
                                            PARALLEL_DIST_SF10,
                                            true,
                                            ObjStoreType::S3));
}

}

}
