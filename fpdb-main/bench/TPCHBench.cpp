//
// Created by Yifei Yang on 4/17/22.
//

#include <doctest/doctest.h>
#include "TestUtil.h"
#include "Globals.h"

/**
 * TPCH test (single compute node, S3)
 *
 * Start Calcite server before running this
 */
namespace fpdb::main::test {

#define SKIP_SUITE false

TEST_SUITE ("tpch-sf10-single_node-parallel" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("tpch-sf10-single_node-parallel-01" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/01.sql"},
                                            PARALLEL_DIST_SF10,
                                            false,
                                            ObjStoreType::S3));
}

TEST_CASE ("tpch-sf10-single_node-parallel-02" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/02.sql"},
                                            PARALLEL_DIST_SF10,
                                            false,
                                            ObjStoreType::S3));
}

TEST_CASE ("tpch-sf10-single_node-parallel-03" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/03.sql"},
                                            PARALLEL_DIST_SF10,
                                            false,
                                            ObjStoreType::S3));
}

TEST_CASE ("tpch-sf10-single_node-parallel-04" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/04.sql"},
                                            PARALLEL_DIST_SF10,
                                            false,
                                            ObjStoreType::S3));
}

TEST_CASE ("tpch-sf10-single_node-parallel-05" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/05.sql"},
                                            PARALLEL_DIST_SF10,
                                            false,
                                            ObjStoreType::S3));
}

TEST_CASE ("tpch-sf10-single_node-parallel-06" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/06.sql"},
                                            PARALLEL_DIST_SF10,
                                            false,
                                            ObjStoreType::S3));
}

TEST_CASE ("tpch-sf10-single_node-parallel-07" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/07.sql"},
                                            PARALLEL_DIST_SF10,
                                            false,
                                            ObjStoreType::S3));
}

TEST_CASE ("tpch-sf10-single_node-parallel-08" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/08.sql"},
                                            PARALLEL_DIST_SF10,
                                            false,
                                            ObjStoreType::S3));
}

TEST_CASE ("tpch-sf10-single_node-parallel-09" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/09.sql"},
                                            PARALLEL_DIST_SF10,
                                            false,
                                            ObjStoreType::S3));
}

TEST_CASE ("tpch-sf10-single_node-parallel-10" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/10.sql"},
                                            PARALLEL_DIST_SF10,
                                            false,
                                            ObjStoreType::S3));
}

TEST_CASE ("tpch-sf10-single_node-parallel-11" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/11.sql"},
                                            PARALLEL_DIST_SF10,
                                            false,
                                            ObjStoreType::S3));
}

TEST_CASE ("tpch-sf10-single_node-parallel-12" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/12.sql"},
                                            PARALLEL_DIST_SF10,
                                            false,
                                            ObjStoreType::S3));
}

TEST_CASE ("tpch-sf10-single_node-parallel-13" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/13.sql"},
                                            PARALLEL_DIST_SF10,
                                            false,
                                            ObjStoreType::S3));
}

TEST_CASE ("tpch-sf10-single_node-parallel-14" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/14.sql"},
                                            PARALLEL_DIST_SF10,
                                            false,
                                            ObjStoreType::S3));
}

TEST_CASE ("tpch-sf10-single_node-parallel-15" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/15.sql"},
                                            PARALLEL_DIST_SF10,
                                            false,
                                            ObjStoreType::S3));
}

TEST_CASE ("tpch-sf10-single_node-parallel-16" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/16.sql"},
                                            PARALLEL_DIST_SF10,
                                            false,
                                            ObjStoreType::S3));
}

TEST_CASE ("tpch-sf10-single_node-parallel-17" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/17.sql"},
                                            PARALLEL_DIST_SF10,
                                            false,
                                            ObjStoreType::S3));
}

TEST_CASE ("tpch-sf10-single_node-parallel-18" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/18.sql"},
                                            PARALLEL_DIST_SF10,
                                            false,
                                            ObjStoreType::S3));
}

TEST_CASE ("tpch-sf10-single_node-parallel-19" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/19.sql"},
                                            PARALLEL_DIST_SF10,
                                            false,
                                            ObjStoreType::S3));
}

TEST_CASE ("tpch-sf10-single_node-parallel-20" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/20.sql"},
                                            PARALLEL_DIST_SF10,
                                            false,
                                            ObjStoreType::S3));
}

TEST_CASE ("tpch-sf10-single_node-parallel-21" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/21.sql"},
                                            PARALLEL_DIST_SF10,
                                            false,
                                            ObjStoreType::S3));
}

TEST_CASE ("tpch-sf10-single_node-parallel-22" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("tpch-sf10/csv/",
                                            {"tpch/original/22.sql"},
                                            PARALLEL_DIST_SF10,
                                            false,
                                            ObjStoreType::S3));
}

}

}
