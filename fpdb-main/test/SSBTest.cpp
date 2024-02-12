//
// Created by Yifei Yang on 11/30/21.
//

#include <doctest/doctest.h>
#include "TestUtil.h"

/**
 * SSB test (single compute node, S3)
 *
 * Start Calcite server before running this
 */
namespace fpdb::main::test {

#define SKIP_SUITE true

TEST_SUITE ("ssb-sf1-single_node-no-parallel" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("ssb-sf1-single_node-no-parallel-original-1.1" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("ssb-sf1-sortlineorder/csv/",
                                            {"ssb/original/1.1.sql"},
                                            1,
                                            false,
                                            ObjStoreType::S3));
}

TEST_CASE ("ssb-sf1-single_node-no-parallel-original-1.2" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("ssb-sf1-sortlineorder/csv/",
                                            {"ssb/original/1.2.sql"},
                                            1,
                                            false,
                                            ObjStoreType::S3));
}

TEST_CASE ("ssb-sf1-single_node-no-parallel-original-1.3" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("ssb-sf1-sortlineorder/csv/",
                                            {"ssb/original/1.3.sql"},
                                            1,
                                            false,
                                            ObjStoreType::S3));
}

TEST_CASE ("ssb-sf1-single_node-no-parallel-original-2.1" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("ssb-sf1-sortlineorder/csv/",
                                            {"ssb/original/2.1.sql"},
                                            1,
                                            false,
                                            ObjStoreType::S3));
}

TEST_CASE ("ssb-sf1-single_node-no-parallel-original-2.2" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("ssb-sf1-sortlineorder/csv/",
                                            {"ssb/original/2.2.sql"},
                                            1,
                                            false,
                                            ObjStoreType::S3));
}

TEST_CASE ("ssb-sf1-single_node-no-parallel-original-2.3" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("ssb-sf1-sortlineorder/csv/",
                                            {"ssb/original/2.3.sql"},
                                            1,
                                            false,
                                            ObjStoreType::S3));
}

TEST_CASE ("ssb-sf1-single_node-no-parallel-original-3.1" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("ssb-sf1-sortlineorder/csv/",
                                            {"ssb/original/3.1.sql"},
                                            1,
                                            false,
                                            ObjStoreType::S3));
}

TEST_CASE ("ssb-sf1-single_node-no-parallel-original-3.2" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("ssb-sf1-sortlineorder/csv/",
                                            {"ssb/original/3.2.sql"},
                                            1,
                                            false,
                                            ObjStoreType::S3));
}

TEST_CASE ("ssb-sf1-single_node-no-parallel-original-3.3" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("ssb-sf1-sortlineorder/csv/",
                                            {"ssb/original/3.3.sql"},
                                            1,
                                            false,
                                            ObjStoreType::S3));
}

TEST_CASE ("ssb-sf1-single_node-no-parallel-original-3.4" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("ssb-sf1-sortlineorder/csv/",
                                            {"ssb/original/3.4.sql"},
                                            1,
                                            false,
                                            ObjStoreType::S3));
}

TEST_CASE ("ssb-sf1-single_node-no-parallel-original-4.1" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("ssb-sf1-sortlineorder/csv/",
                                            {"ssb/original/4.1.sql"},
                                            1,
                                            false,
                                            ObjStoreType::S3));
}

TEST_CASE ("ssb-sf1-single_node-no-parallel-original-4.2" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("ssb-sf1-sortlineorder/csv/",
                                            {"ssb/original/4.2.sql"},
                                            1,
                                            false,
                                            ObjStoreType::S3));
}

TEST_CASE ("ssb-sf1-single_node-no-parallel-original-4.3" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("ssb-sf1-sortlineorder/csv/",
                                            {"ssb/original/4.3.sql"},
                                            1,
                                            false,
                                            ObjStoreType::S3));
}

TEST_CASE ("ssb-sf1-single_node-no-parallel-generated-1" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("ssb-sf1-sortlineorder/csv/",
                                            {"ssb/generated/1.sql"},
                                            1,
                                            false,
                                            ObjStoreType::S3));
}

TEST_CASE ("ssb-sf1-single_node-no-parallel-generated-2" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("ssb-sf1-sortlineorder/csv/",
                                            {"ssb/generated/2.sql"},
                                            1,
                                            false,
                                            ObjStoreType::S3));
}

TEST_CASE ("ssb-sf1-single_node-no-parallel-generated-3" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("ssb-sf1-sortlineorder/csv/",
                                            {"ssb/generated/3.sql"},
                                            1,
                                            false,
                                            ObjStoreType::S3));
}

TEST_CASE ("ssb-sf1-single_node-no-parallel-generated-4" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("ssb-sf1-sortlineorder/csv/",
                                            {"ssb/generated/4.sql"},
                                            1,
                                            false,
                                            ObjStoreType::S3));
}

TEST_CASE ("ssb-sf1-single_node-no-parallel-generated-5" * doctest::skip(false || SKIP_SUITE)) {
  REQUIRE(TestUtil::e2eNoStartCalciteServer("ssb-sf1-sortlineorder/csv/",
                                            {"ssb/generated/5.sql"},
                                            1,
                                            false,
                                            ObjStoreType::S3));
}
}

}
