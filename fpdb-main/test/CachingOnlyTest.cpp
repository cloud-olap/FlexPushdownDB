//
// Created by Yifei Yang on 3/26/22.
//

#include <doctest/doctest.h>
#include "TestUtil.h"
#include "Globals.h"

using namespace fpdb::util;

/**
 * Test of caching-only execution, using TPC-H dataset
 * Using the same setup as TPCHFPDBStoreSameNodeTest
 * Start Calcite server before running this
 */
namespace fpdb::main::test {

#define SKIP_SUITE false

TEST_SUITE ("caching-only" * doctest::skip(SKIP_SUITE)) {

constexpr std::string_view cachingOnlyTestQueryFileName = "caching_only_test.sql";
constexpr std::string_view cachingOnlyTestPartialCachingQueryFileName = "caching_only_test_partial_caching.sql";
constexpr std::string_view cachingOnlyTestNoCachingQueryFileName = "caching_only_test_no_caching.sql";

constexpr std::string_view cachingOnlyTestQuery = "select\n"
                                                  "    l_returnflag,\n"
                                                  "    l_linestatus,\n"
                                                  "    l_quantity,\n"
                                                  "    l_extendedprice\n"
                                                  "from\n"
                                                  "\tlineitem\n"
                                                  "where\n"
                                                  "\tl_discount <= 0.02\n"
                                                  "order by\n"
                                                  "    l_returnflag,\n"
                                                  "    l_linestatus\n"
                                                  "limit 10";

constexpr std::string_view cachingOnlyTestPartialCachingQuery = "select\n"
                                                                "    l_returnflag,\n"
                                                                "    l_quantity\n"
                                                                "from\n"
                                                                "\tlineitem\n"
                                                                "where\n"
                                                                "\tl_discount <= 0.02\n"
                                                                "order by\n"
                                                                "    l_returnflag\n"
                                                                "limit 10";

constexpr std::string_view cachingOnlyTestNoCachingQuery = "select\n"
                                                           "    p_partkey,\n"
                                                           "    p_size\n"
                                                           "from\n"
                                                           "\tpart\n"
                                                           "where\n"
                                                           "\tp_size <= 2\n"
                                                           "order by\n"
                                                           "    p_size\n"
                                                           "limit 10";

// run cachingOnlyTestQuery twice
TEST_CASE ("caching-only-all-cached" * doctest::skip(false || SKIP_SUITE)) {
  // write query to file
  TestUtil::writeQueryToFile(cachingOnlyTestQueryFileName.data(), cachingOnlyTestQuery.data());

  // test
  TestUtil::TestUtil::TestUtil::startFPDBStoreServer();
  TestUtil testUtil("tpch-sf0.01/parquet/",
                    {cachingOnlyTestQueryFileName.data(),
                     cachingOnlyTestQueryFileName.data()},
                    PARALLEL_FPDB_STORE_SAME_NODE,
                    false,
                    ObjStoreType::FPDB_STORE,
                    Mode::cachingOnlyMode(),
                    CachingPolicyType::LFU,
                    1L * 1024 * 1024 * 1024);

  REQUIRE_NOTHROW(testUtil.runTest());
  REQUIRE_EQ(testUtil.getCrtQueryHitRatio(), 1.0);

  TestUtil::stopFPDBStoreServer();

  // clear query file
  TestUtil::removeQueryFile(cachingOnlyTestQueryFileName.data());
}

// run cachingOnlyTestPartialCachingQuery, then cachingOnlyTestQuery
TEST_CASE ("caching-only-partial-cached" * doctest::skip(false || SKIP_SUITE)) {
  // write query to file
  TestUtil::writeQueryToFile(cachingOnlyTestPartialCachingQueryFileName.data(), cachingOnlyTestPartialCachingQuery.data());
  TestUtil::writeQueryToFile(cachingOnlyTestQueryFileName.data(), cachingOnlyTestQuery.data());

  // test
  TestUtil::TestUtil::TestUtil::startFPDBStoreServer();
  TestUtil testUtil("tpch-sf0.01/parquet/",
                    {cachingOnlyTestPartialCachingQueryFileName.data(),
                     cachingOnlyTestQueryFileName.data()},
                    PARALLEL_FPDB_STORE_SAME_NODE,
                    false,
                    ObjStoreType::FPDB_STORE,
                    Mode::cachingOnlyMode(),
                    CachingPolicyType::LFU,
                    1L * 1024 * 1024 * 1024);

  REQUIRE_NOTHROW(testUtil.runTest());
  REQUIRE_GT(testUtil.getCrtQueryHitRatio(), 0.0);
  REQUIRE_LT(testUtil.getCrtQueryHitRatio(), 1.0);

  TestUtil::stopFPDBStoreServer();

  // clear query file
  TestUtil::removeQueryFile(cachingOnlyTestPartialCachingQueryFileName.data());
  TestUtil::removeQueryFile(cachingOnlyTestQueryFileName.data());
}

// run cachingOnlyTestNoCachingQuery, then cachingOnlyTestQuery
TEST_CASE ("caching-only-none-cached" * doctest::skip(false || SKIP_SUITE)) {
  // write query to file
  TestUtil::writeQueryToFile(cachingOnlyTestNoCachingQueryFileName.data(), cachingOnlyTestNoCachingQuery.data());
  TestUtil::writeQueryToFile(cachingOnlyTestQueryFileName.data(), cachingOnlyTestQuery.data());

  // test
  TestUtil::TestUtil::TestUtil::startFPDBStoreServer();
  TestUtil testUtil("tpch-sf0.01/parquet/",
                    {cachingOnlyTestNoCachingQueryFileName.data(),
                     cachingOnlyTestQueryFileName.data()},
                    PARALLEL_FPDB_STORE_SAME_NODE,
                    false,
                    ObjStoreType::FPDB_STORE,
                    Mode::cachingOnlyMode(),
                    CachingPolicyType::LFU,
                    1L * 1024 * 1024 * 1024);

  REQUIRE_NOTHROW(testUtil.runTest());
  REQUIRE_EQ(testUtil.getCrtQueryHitRatio(), 0.0);

  TestUtil::stopFPDBStoreServer();

  // clear query file
  TestUtil::removeQueryFile(cachingOnlyTestNoCachingQueryFileName.data());
  TestUtil::removeQueryFile(cachingOnlyTestQueryFileName.data());
}

}

}
