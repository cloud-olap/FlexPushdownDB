//
// Created by Yifei Yang on 4/8/22.
//

#include <doctest/doctest.h>
#include "TestUtil.h"
#include "BitmapPushdownTestUtil.h"
#include "Globals.h"
#include <fpdb/executor/physical/Globals.h>

using namespace fpdb::util;

/**
 * Test of bitmap pushdown (hybrid execution), using TPC-H dataset
 * Using the same setup as HybridTest
 * Start Calcite server before running this
 */
namespace fpdb::main::test {

#define SKIP_SUITE false

TEST_SUITE ("bitmap-pushdown" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("bitmap-pushdown-all-cached" * doctest::skip(false || SKIP_SUITE)) {
  fpdb::executor::physical::ENABLE_FILTER_BITMAP_PUSHDOWN = true;
  std::string testQueryFileName = "test.sql";
  std::string testQuery = "select\n"
                          "    l_returnflag, l_linestatus, l_quantity, l_extendedprice\n"
                          "from\n"
                          "    lineitem\n"
                          "where\n"
                          "    l_discount <= 0.02\n"
                          "order by\n"
                          "    l_returnflag, l_linestatus\n"
                          "limit 10";

  // write query to file
  TestUtil::writeQueryToFile(testQueryFileName, testQuery);

  // test
  TestUtil::startFPDBStoreServer();
  TestUtil testUtil("tpch-sf0.01/parquet/",
                    {testQueryFileName,
                     testQueryFileName},
                    PARALLEL_FPDB_STORE_SAME_NODE,
                    false,
                    ObjStoreType::FPDB_STORE,
                    Mode::hybridMode(),
                    CachingPolicyType::LFU,
                    1L * 1024 * 1024 * 1024);

  REQUIRE_NOTHROW(testUtil.runTest());
  REQUIRE_EQ(testUtil.getCrtQueryHitRatio(), 1.0);

  TestUtil::stopFPDBStoreServer();

  // clear query file
  fpdb::executor::physical::ENABLE_FILTER_BITMAP_PUSHDOWN = false;
  TestUtil::removeQueryFile(testQueryFileName);
}

TEST_CASE ("bitmap-pushdown-none-cached" * doctest::skip(false || SKIP_SUITE)) {
  fpdb::executor::physical::ENABLE_FILTER_BITMAP_PUSHDOWN = true;
  std::string cachingQueryFileName = "caching.sql";
  std::string testQueryFileName = "test.sql";
  std::string cachingQuery = "select\n"
                             "    p_partkey, p_size\n"
                             "from\n"
                             "    part\n"
                             "where\n"
                             "    p_size <= 2\n"
                             "order by\n"
                             "    p_size\n"
                             "limit 10";
  std::string testQuery = "select\n"
                          "    l_returnflag, l_linestatus, l_quantity, l_extendedprice\n"
                          "from\n"
                          "    lineitem\n"
                          "where\n"
                          "    l_discount <= 0.02\n"
                          "order by\n"
                          "    l_returnflag, l_linestatus\n"
                          "limit 10";

  // write query to file
  TestUtil::writeQueryToFile(cachingQueryFileName, cachingQuery);
  TestUtil::writeQueryToFile(testQueryFileName, testQuery);

  // test
  TestUtil::startFPDBStoreServer();
  TestUtil testUtil("tpch-sf0.01/parquet/",
                    {cachingQueryFileName,
                     testQueryFileName},
                    PARALLEL_FPDB_STORE_SAME_NODE,
                    false,
                    ObjStoreType::FPDB_STORE,
                    Mode::hybridMode(),
                    CachingPolicyType::LFU,
                    1L * 1024 * 1024 * 1024);

  REQUIRE_NOTHROW(testUtil.runTest());
  REQUIRE_EQ(testUtil.getCrtQueryHitRatio(), 0.0);

  TestUtil::stopFPDBStoreServer();

  // clear query file
  fpdb::executor::physical::ENABLE_FILTER_BITMAP_PUSHDOWN = false;
  TestUtil::removeQueryFile(cachingQueryFileName);
  TestUtil::removeQueryFile(testQueryFileName);
}

/**
 * The case when bitmap is constructed at compute side, and both sides have some project column groups
 */
TEST_CASE ("bitmap-pushdown-partial-cached-compute-bitmap-both-project" * doctest::skip(false || SKIP_SUITE)) {
  fpdb::executor::physical::ENABLE_FILTER_BITMAP_PUSHDOWN = true;
  std::string cachingQueryFileName = "caching.sql";
  std::string testQueryFileName = "test.sql";
  std::string cachingQuery = "select\n"
                             "    l_returnflag, l_quantity\n"
                             "from\n"
                             "    lineitem\n"
                             "where\n"
                             "    l_discount <= 0.02\n"
                             "order by\n"
                             "    l_returnflag\n"
                             "limit 10";
  std::string testQuery = "select\n"
                          "    l_returnflag, l_linestatus, l_quantity, l_extendedprice\n"
                          "from\n"
                          "    lineitem\n"
                          "where\n"
                          "    l_discount <= 0.02\n"
                          "order by\n"
                          "    l_returnflag, l_linestatus\n"
                          "limit 10";

  // write query to file
  TestUtil::writeQueryToFile(cachingQueryFileName, cachingQuery);
  TestUtil::writeQueryToFile(testQueryFileName, testQuery);

  // test
  TestUtil::startFPDBStoreServer();
  TestUtil testUtil("tpch-sf0.01/parquet/",
                    {cachingQueryFileName,
                     testQueryFileName},
                    PARALLEL_FPDB_STORE_SAME_NODE,
                    false,
                    ObjStoreType::FPDB_STORE,
                    Mode::hybridMode(),
                    CachingPolicyType::LFU,
                    1L * 1024 * 1024 * 1024);
  // fix cache layout after caching query, otherwise it keeps fetching new segments
  // which is not intra-partition hybrid execution (i.e. hit data + loaded new cache data is enough for execution,
  // pushdown part actually does nothing)
  testUtil.setFixLayoutIndices({0});

  REQUIRE_NOTHROW(testUtil.runTest());
  REQUIRE_GT(testUtil.getCrtQueryHitRatio(), 0.0);
  REQUIRE_LT(testUtil.getCrtQueryHitRatio(), 1.0);

  TestUtil::stopFPDBStoreServer();

  // clear query file
  fpdb::executor::physical::ENABLE_FILTER_BITMAP_PUSHDOWN = false;
  TestUtil::removeQueryFile(cachingQueryFileName);
  TestUtil::removeQueryFile(testQueryFileName);
}

/**
 * The case when bitmap is constructed at compute side, and only storage side has some project column groups
 */
TEST_CASE ("bitmap-pushdown-partial-cached-compute-bitmap-only-storage-project" * doctest::skip(false || SKIP_SUITE)) {
  fpdb::executor::physical::ENABLE_FILTER_BITMAP_PUSHDOWN = true;
  std::string cachingQueryFileName = "caching.sql";
  std::string testQueryFileName = "test.sql";
  std::string cachingQuery = "select\n"
                             "    l_shipdate, l_discount\n"
                             "from\n"
                             "    lineitem\n"
                             "where\n"
                             "    l_discount <= 0.02\n"
                             "order by\n"
                             "    l_discount\n"
                             "limit 10";
  std::string testQuery = "select\n"
                          "    l_returnflag, l_linestatus, l_quantity, l_extendedprice\n"
                          "from\n"
                          "    lineitem\n"
                          "where\n"
                          "    l_discount <= 0.02\n"
                          "order by\n"
                          "    l_returnflag, l_linestatus\n"
                          "limit 10";

  // write query to file
  TestUtil::writeQueryToFile(cachingQueryFileName, cachingQuery);
  TestUtil::writeQueryToFile(testQueryFileName, testQuery);

  // test
  TestUtil::startFPDBStoreServer();
  TestUtil testUtil("tpch-sf0.01/parquet/",
                    {cachingQueryFileName,
                     testQueryFileName},
                    PARALLEL_FPDB_STORE_SAME_NODE,
                    false,
                    ObjStoreType::FPDB_STORE,
                    Mode::hybridMode(),
                    CachingPolicyType::LFU,
                    1L * 1024 * 1024 * 1024);
  // fix cache layout after caching query, otherwise it keeps fetching new segments
  // which is not intra-partition hybrid execution (i.e. hit data + loaded new cache data is enough for execution,
  // pushdown part actually does nothing)
  testUtil.setFixLayoutIndices({0});

  REQUIRE_NOTHROW(testUtil.runTest());
  REQUIRE_GT(testUtil.getCrtQueryHitRatio(), 0.0);
  REQUIRE_LT(testUtil.getCrtQueryHitRatio(), 1.0);

  TestUtil::stopFPDBStoreServer();

  // clear query file
  fpdb::executor::physical::ENABLE_FILTER_BITMAP_PUSHDOWN = false;
  TestUtil::removeQueryFile(cachingQueryFileName);
  TestUtil::removeQueryFile(testQueryFileName);
}

/**
 * The case when bitmap is constructed at storage side, and both sides have some project column groups
 */
TEST_CASE ("bitmap-pushdown-partial-cached-storage-bitmap-both-project" * doctest::skip(false || SKIP_SUITE)) {
  fpdb::executor::physical::ENABLE_FILTER_BITMAP_PUSHDOWN = true;
  std::string cachingQueryFileName = "caching.sql";
  std::string testQueryFileName = "test.sql";
  std::string cachingQuery = "select\n"
                             "    l_returnflag, l_quantity\n"
                             "from\n"
                             "    lineitem\n"
                             "where\n"
                             "    l_quantity < 20\n"
                             "order by\n"
                             "    l_returnflag\n"
                             "limit 10";
  std::string testQuery = "select\n"
                          "    l_returnflag, l_linestatus, l_quantity, l_extendedprice\n"
                          "from\n"
                          "    lineitem\n"
                          "where\n"
                          "    l_discount <= 0.02\n"
                          "order by\n"
                          "    l_returnflag, l_linestatus\n"
                          "limit 10";

  // write query to file
  TestUtil::writeQueryToFile(cachingQueryFileName, cachingQuery);
  TestUtil::writeQueryToFile(testQueryFileName, testQuery);

  // test
  TestUtil::startFPDBStoreServer();
  TestUtil testUtil("tpch-sf0.01/parquet/",
                    {cachingQueryFileName,
                     testQueryFileName},
                    PARALLEL_FPDB_STORE_SAME_NODE,
                    false,
                    ObjStoreType::FPDB_STORE,
                    Mode::hybridMode(),
                    CachingPolicyType::LFU,
                    1L * 1024 * 1024 * 1024);
  // fix cache layout after caching query, otherwise it keeps fetching new segments
  // which is not intra-partition hybrid execution (i.e. hit data + loaded new cache data is enough for execution,
  // pushdown part actually does nothing)
  testUtil.setFixLayoutIndices({0});

  REQUIRE_NOTHROW(testUtil.runTest());
  REQUIRE_GT(testUtil.getCrtQueryHitRatio(), 0.0);
  REQUIRE_LT(testUtil.getCrtQueryHitRatio(), 1.0);

  TestUtil::stopFPDBStoreServer();

  // clear query file
  fpdb::executor::physical::ENABLE_FILTER_BITMAP_PUSHDOWN = false;
  TestUtil::removeQueryFile(cachingQueryFileName);
  TestUtil::removeQueryFile(testQueryFileName);
}

/**
 * The case when bitmap is constructed at storage side, and only compute side has some project column groups
 */
TEST_CASE ("bitmap-pushdown-partial-cached-storage-bitmap-only-compute-project" * doctest::skip(false || SKIP_SUITE)) {
  fpdb::executor::physical::ENABLE_FILTER_BITMAP_PUSHDOWN = true;
  std::string cachingQueryFileName = "caching.sql";
  std::string testQueryFileName = "test.sql";
  std::string cachingQuery = "select\n"
                             "    l_returnflag, l_linestatus, l_quantity, l_extendedprice\n"
                             "from\n"
                             "    lineitem\n"
                             "where\n"
                             "    l_quantity < 20\n"
                             "order by\n"
                             "    l_returnflag\n"
                             "limit 10";
  std::string testQuery = "select\n"
                          "    l_returnflag, l_linestatus, l_quantity, l_extendedprice\n"
                          "from\n"
                          "    lineitem\n"
                          "where\n"
                          "    l_discount <= 0.02\n"
                          "order by\n"
                          "    l_returnflag, l_linestatus\n"
                          "limit 10";

  // write query to file
  TestUtil::writeQueryToFile(cachingQueryFileName, cachingQuery);
  TestUtil::writeQueryToFile(testQueryFileName, testQuery);

  // test
  TestUtil::startFPDBStoreServer();
  TestUtil testUtil("tpch-sf0.01/parquet/",
                    {cachingQueryFileName,
                     testQueryFileName},
                    PARALLEL_FPDB_STORE_SAME_NODE,
                    false,
                    ObjStoreType::FPDB_STORE,
                    Mode::hybridMode(),
                    CachingPolicyType::LFU,
                    1L * 1024 * 1024 * 1024);
  // fix cache layout after caching query, otherwise it keeps fetching new segments
  // which is not intra-partition hybrid execution (i.e. hit data + loaded new cache data is enough for execution,
  // pushdown part actually does nothing)
  testUtil.setFixLayoutIndices({0});

  REQUIRE_NOTHROW(testUtil.runTest());
  REQUIRE_GT(testUtil.getCrtQueryHitRatio(), 0.0);
  REQUIRE_LT(testUtil.getCrtQueryHitRatio(), 1.0);

  TestUtil::stopFPDBStoreServer();

  // clear query file
  fpdb::executor::physical::ENABLE_FILTER_BITMAP_PUSHDOWN = false;
  TestUtil::removeQueryFile(cachingQueryFileName);
  TestUtil::removeQueryFile(testQueryFileName);
}

}

TEST_SUITE ("bitmap-pushdown-test-benchmark-query-storage-bitmap" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("bitmap-pushdown-test-benchmark-query-storage-bitmap-ssb-1.1" * doctest::skip(false || SKIP_SUITE)) {
  std::string cachingQuery = "select lo_extendedprice, lo_discount, lo_orderdate\n"
                             "from lineorder";
  std::string testQueryTemplate = "select sum(lo_extendedprice * lo_discount) as revenue\n"
                                  "from lineorder,\n"
                                  "     \"date\"\n"
                                  "where lo_orderdate = d_datekey\n"
                                  "  and d_year = 1992\n"
                                  "  and lo_discount between 0 and 10\n"
                                  "  and lo_quantity < {}";
  std::vector<std::string> testQueries;
  std::vector<std::string> testQueryFileNames;
  for (double sel = 0.0; sel < 1.0; sel += 0.1) {
    testQueries.emplace_back(fmt::format(testQueryTemplate, (int)(50 * sel)));
    testQueryFileNames.emplace_back(fmt::format("ssb-1.1-sel-{:.1f}", sel));
  }
  BitmapPushdownTestUtil::run_bitmap_pushdown_benchmark_query(cachingQuery,
                                                              testQueries,
                                                              testQueryFileNames,
                                                              true,
                                                              "0.01",
                                                              PARALLEL_FPDB_STORE_SAME_NODE,
                                                              true);
}

TEST_CASE ("bitmap-pushdown-test-benchmark-query-storage-bitmap-tpch-03" * doctest::skip(false || SKIP_SUITE)) {
  std::string cachingQuery = "select l_orderkey, l_extendedprice, l_discount\n"
                             "from lineitem";
  std::string testQueryTemplate = "select\n"
                                  "  l.l_orderkey,\n"
                                  "  sum(l.l_extendedprice * (1 - l.l_discount)) as revenue,\n"
                                  "  o.o_orderdate,\n"
                                  "  o.o_shippriority\n"
                                  "from\n"
                                  "  customer c,\n"
                                  "  orders o,\n"
                                  "  lineitem l\n"
                                  "where\n"
                                  "  c.c_mktsegment = 'HOUSEHOLD'\n"
                                  "  and c.c_custkey = o.o_custkey\n"
                                  "  and l.l_orderkey = o.o_orderkey\n"
                                  "  and o.o_orderdate < date '1992-03-25'\n"
                                  "  and l.l_shipdate > date '1991-01-01'\n"
                                  "  and l.l_quantity < {}\n"
                                  "group by\n"
                                  "  l.l_orderkey,\n"
                                  "  o.o_orderdate,\n"
                                  "  o.o_shippriority\n"
                                  "order by\n"
                                  "  revenue desc,\n"
                                  "  o.o_orderdate\n"
                                  "limit 10";
  std::vector<std::string> testQueries;
  std::vector<std::string> testQueryFileNames;
  for (double sel = 0.0; sel < 1.0; sel += 0.1) {
    testQueries.emplace_back(fmt::format(testQueryTemplate, (int)(50 * sel)));
    testQueryFileNames.emplace_back(fmt::format("tpch-03-sel-{:.1f}", sel));
  }
  BitmapPushdownTestUtil::run_bitmap_pushdown_benchmark_query(cachingQuery,
                                                              testQueries,
                                                              testQueryFileNames,
                                                              false,
                                                              "0.01",
                                                              PARALLEL_FPDB_STORE_SAME_NODE,
                                                              true);
}

TEST_CASE ("bitmap-pushdown-test-benchmark-query-storage-bitmap-tpch-04" * doctest::skip(false || SKIP_SUITE)) {
  std::string cachingQuery = "select l_orderkey\n"
                             "from lineitem";
  std::string testQueryTemplate = "select\n"
                                  "  o.o_orderpriority,\n"
                                  "  count(*) as order_count\n"
                                  "from\n"
                                  "  orders o\n"
                                  "where\n"
                                  "  o.o_orderdate >= date '1996-10-01'\n"
                                  "  and o.o_orderdate < date '1996-10-01' + interval '3' month\n"
                                  "  and\n"
                                  "  exists (\n"
                                  "    select\n"
                                  "      *\n"
                                  "    from\n"
                                  "      lineitem l\n"
                                  "    where\n"
                                  "      l.l_orderkey = o.o_orderkey\n"
                                  "      and l.l_quantity < {}\n"
                                  "      and l.l_commitdate > date '1991-01-01'\n"
                                  "      and l.l_receiptdate > date '1991-01-01'\n"
                                  "  )\n"
                                  "group by\n"
                                  "  o.o_orderpriority\n"
                                  "order by\n"
                                  "  o.o_orderpriority";
  std::vector<std::string> testQueries;
  std::vector<std::string> testQueryFileNames;
  for (double sel = 0.0; sel < 1.0; sel += 0.1) {
    testQueries.emplace_back(fmt::format(testQueryTemplate, (int)(50 * sel)));
    testQueryFileNames.emplace_back(fmt::format("tpch-04-sel-{:.1f}", sel));
  }
  BitmapPushdownTestUtil::run_bitmap_pushdown_benchmark_query(cachingQuery,
                                                              testQueries,
                                                              testQueryFileNames,
                                                              false,
                                                              "0.01",
                                                              PARALLEL_FPDB_STORE_SAME_NODE,
                                                              true);
}

TEST_CASE ("bitmap-pushdown-test-benchmark-query-storage-bitmap-tpch-12" * doctest::skip(false || SKIP_SUITE)) {
  std::string cachingQuery = "select l_orderkey, l_shipmode\n"
                             "from lineitem";
  std::string testQueryTemplate = "select\n"
                                  "  l.l_shipmode,\n"
                                  "  sum(case\n"
                                  "    when o.o_orderpriority = '1-URGENT'\n"
                                  "      or o.o_orderpriority = '2-HIGH'\n"
                                  "      then 1\n"
                                  "    else 0\n"
                                  "  end) as high_line_count,\n"
                                  "  sum(case\n"
                                  "    when o.o_orderpriority <> '1-URGENT'\n"
                                  "      and o.o_orderpriority <> '2-HIGH'\n"
                                  "      then 1\n"
                                  "    else 0\n"
                                  "  end) as low_line_count\n"
                                  "from\n"
                                  "  orders o,\n"
                                  "  lineitem l\n"
                                  "where\n"
                                  "  o.o_orderkey = l.l_orderkey\n"
                                  "  and o.o_orderdate >= date '1993-10-01'\n"
                                  "  and o.o_orderdate < date '1993-10-01' + interval '3' month"
                                  "  and l.l_quantity < {}\n"
                                  "  and l.l_shipmode >= '0'\n"
                                  "  and l.l_commitdate > date '1991-01-01'\n"
                                  "  and l.l_shipdate > date '1991-01-01'\n"
                                  "  and l.l_receiptdate > date '1991-01-01'\n"
                                  "group by\n"
                                  "  l.l_shipmode\n"
                                  "order by\n"
                                  "  l.l_shipmode";
  std::vector<std::string> testQueries;
  std::vector<std::string> testQueryFileNames;
  for (double sel = 0.0; sel < 1.0; sel += 0.1) {
    testQueries.emplace_back(fmt::format(testQueryTemplate, (int)(50 * sel)));
    testQueryFileNames.emplace_back(fmt::format("tpch-12-sel-{:.1f}", sel));
  }
  BitmapPushdownTestUtil::run_bitmap_pushdown_benchmark_query(cachingQuery,
                                                              testQueries,
                                                              testQueryFileNames,
                                                              false,
                                                              "0.01",
                                                              PARALLEL_FPDB_STORE_SAME_NODE,
                                                              true);
}

TEST_CASE ("bitmap-pushdown-test-benchmark-query-storage-bitmap-tpch-14" * doctest::skip(false || SKIP_SUITE)) {
  std::string cachingQuery = "select l_partkey, l_extendedprice, l_discount\n"
                             "from lineitem";
  std::string testQueryTemplate = "select\n"
                                  "  100.00 * sum(case\n"
                                  "    when p.p_type like 'PROMO%'\n"
                                  "      then l.l_extendedprice * (1 - l.l_discount)\n"
                                  "    else 0\n"
                                  "  end) / (sum(l.l_extendedprice * (1 - l.l_discount)) + 1) as promo_revenue\n"
                                  "from\n"
                                  "  lineitem l,\n"
                                  "  part p\n"
                                  "where\n"
                                  "  l.l_partkey = p.p_partkey\n"
                                  "  and p.p_brand = 'Brand#41'"
                                  "  and l.l_quantity < {}\n"
                                  "  and l.l_shipdate >= date '1991-01-01'\n"
                                  "  and l.l_shipdate < date '1999-01-01'";
  std::vector<std::string> testQueries;
  std::vector<std::string> testQueryFileNames;
  for (double sel = 0.0; sel < 1.0; sel += 0.1) {
    testQueries.emplace_back(fmt::format(testQueryTemplate, (int)(50 * sel)));
    testQueryFileNames.emplace_back(fmt::format("tpch-14-sel-{:.1f}", sel));
  }
  BitmapPushdownTestUtil::run_bitmap_pushdown_benchmark_query(cachingQuery,
                                                              testQueries,
                                                              testQueryFileNames,
                                                              false,
                                                              "0.01",
                                                              PARALLEL_FPDB_STORE_SAME_NODE,
                                                              true);
}

TEST_CASE ("bitmap-pushdown-test-benchmark-query-storage-bitmap-tpch-19" * doctest::skip(false || SKIP_SUITE)) {
  std::string cachingQuery = "select l_partkey, l_extendedprice, l_discount, l_quantity\n"
                             "from lineitem";
  std::string testQueryTemplate = "select\n"
                                  "  sum(l.l_extendedprice* (1 - l.l_discount)) as revenue\n"
                                  "from\n"
                                  "  lineitem l,\n"
                                  "  part p\n"
                                  "where\n"
                                  "  (\n"
                                  "    p.p_partkey = l.l_partkey\n"
                                  "    and p.p_brand = 'Brand#41'\n"
                                  "    and p.p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')\n"
                                  "    and l.l_quantity >= 0 and l.l_quantity <= {0}\n"
                                  "    and p.p_size between 1 and 5\n"
                                  "    and l.l_shipmode >= '0'\n"
                                  "    and l.l_shipinstruct >= '0'\n"
                                  "  )\n"
                                  "  or\n"
                                  "  (\n"
                                  "    p.p_partkey = l.l_partkey\n"
                                  "    and p.p_brand = 'Brand#13'\n"
                                  "    and p.p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')\n"
                                  "    and l.l_quantity >= 0 and l.l_quantity <= {0}\n"
                                  "    and p.p_size between 6 and 10\n"
                                  "    and l.l_shipmode >= '0'\n"
                                  "    and l.l_shipinstruct >= '0'\n"
                                  "  )\n"
                                  "  or\n"
                                  "  (\n"
                                  "    p.p_partkey = l.l_partkey\n"
                                  "    and p.p_brand = 'Brand#55'\n"
                                  "    and p.p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')\n"
                                  "    and l.l_quantity >= 0 and l.l_quantity <= {0}\n"
                                  "    and p.p_size between 11 and 15\n"
                                  "    and l.l_shipmode >= '0'\n"
                                  "    and l.l_shipinstruct >= '0'\n"
                                  "  )";
  std::vector<std::string> testQueries;
  std::vector<std::string> testQueryFileNames;
  for (double sel = 0.0; sel < 1.0; sel += 0.1) {
    testQueries.emplace_back(fmt::format(testQueryTemplate, (int)(50 * sel)));
    testQueryFileNames.emplace_back(fmt::format("tpch-19-sel-{:.1f}", sel));
  }
  BitmapPushdownTestUtil::run_bitmap_pushdown_benchmark_query(cachingQuery,
                                                              testQueries,
                                                              testQueryFileNames,
                                                              false,
                                                              "0.01",
                                                              PARALLEL_FPDB_STORE_SAME_NODE,
                                                              true);
}

}

TEST_SUITE ("bitmap-pushdown-test-benchmark-query-compute-bitmap" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("bitmap-pushdown-test-benchmark-query-compute-bitmap-ssb-1.1" * doctest::skip(false || SKIP_SUITE)) {
  std::string cachingQuery = "select lo_discount, lo_quantity\n"
                             "from lineorder";
  std::string testQueryTemplate = "select sum(lo_extendedprice * lo_discount) as revenue\n"
                                  "from lineorder,\n"
                                  "     \"date\"\n"
                                  "where lo_orderdate = d_datekey\n"
                                  "  and d_year = 1992\n"
                                  "  and lo_discount between 0 and 10\n"
                                  "  and lo_quantity < {}";
  std::vector<std::string> testQueries;
  std::vector<std::string> testQueryFileNames;
  for (double sel = 0.0; sel < 1.0; sel += 0.1) {
    testQueries.emplace_back(fmt::format(testQueryTemplate, (int)(50 * sel)));
    testQueryFileNames.emplace_back(fmt::format("ssb-1.1-sel-{:.1f}", sel));
  }
  BitmapPushdownTestUtil::run_bitmap_pushdown_benchmark_query(cachingQuery,
                                                              testQueries,
                                                              testQueryFileNames,
                                                              true,
                                                              "0.01",
                                                              PARALLEL_FPDB_STORE_SAME_NODE,
                                                              true);
}

TEST_CASE ("bitmap-pushdown-test-benchmark-query-compute-bitmap-tpch-03" * doctest::skip(false || SKIP_SUITE)) {
  std::string cachingQuery = "select l_shipdate, l_quantity\n"
                             "from lineitem";
  std::string testQueryTemplate = "select\n"
                                  "  l.l_orderkey,\n"
                                  "  sum(l.l_extendedprice * (1 - l.l_discount)) as revenue,\n"
                                  "  o.o_orderdate,\n"
                                  "  o.o_shippriority\n"
                                  "from\n"
                                  "  customer c,\n"
                                  "  orders o,\n"
                                  "  lineitem l\n"
                                  "where\n"
                                  "  c.c_mktsegment = 'HOUSEHOLD'\n"
                                  "  and c.c_custkey = o.o_custkey\n"
                                  "  and l.l_orderkey = o.o_orderkey\n"
                                  "  and o.o_orderdate < date '1992-03-25'\n"
                                  "  and l.l_shipdate > date '1991-01-01'\n"
                                  "  and l.l_quantity < {}\n"
                                  "group by\n"
                                  "  l.l_orderkey,\n"
                                  "  o.o_orderdate,\n"
                                  "  o.o_shippriority\n"
                                  "order by\n"
                                  "  revenue desc,\n"
                                  "  o.o_orderdate\n"
                                  "limit 10";
  std::vector<std::string> testQueries;
  std::vector<std::string> testQueryFileNames;
  for (double sel = 0.0; sel < 1.0; sel += 0.1) {
    testQueries.emplace_back(fmt::format(testQueryTemplate, (int)(50 * sel)));
    testQueryFileNames.emplace_back(fmt::format("tpch-03-sel-{:.1f}", sel));
  }
  BitmapPushdownTestUtil::run_bitmap_pushdown_benchmark_query(cachingQuery,
                                                              testQueries,
                                                              testQueryFileNames,
                                                              false,
                                                              "0.01",
                                                              PARALLEL_FPDB_STORE_SAME_NODE,
                                                              true);
}

TEST_CASE ("bitmap-pushdown-test-benchmark-query-compute-bitmap-tpch-04" * doctest::skip(false || SKIP_SUITE)) {
  std::string cachingQuery = "select l_commitdate, l_receiptdate, l_quantity\n"
                             "from lineitem";
  std::string testQueryTemplate = "select\n"
                                  "  o.o_orderpriority,\n"
                                  "  count(*) as order_count\n"
                                  "from\n"
                                  "  orders o\n"
                                  "where\n"
                                  "  o.o_orderdate >= date '1996-10-01'\n"
                                  "  and o.o_orderdate < date '1996-10-01' + interval '3' month\n"
                                  "  and\n"
                                  "  exists (\n"
                                  "    select\n"
                                  "      *\n"
                                  "    from\n"
                                  "      lineitem l\n"
                                  "    where\n"
                                  "      l.l_orderkey = o.o_orderkey\n"
                                  "      and l.l_quantity < {}\n"
                                  "      and l.l_commitdate > date '1991-01-01'\n"
                                  "      and l.l_receiptdate > date '1991-01-01'\n"
                                  "  )\n"
                                  "group by\n"
                                  "  o.o_orderpriority\n"
                                  "order by\n"
                                  "  o.o_orderpriority";
  std::vector<std::string> testQueries;
  std::vector<std::string> testQueryFileNames;
  for (double sel = 0.0; sel < 1.0; sel += 0.1) {
    testQueries.emplace_back(fmt::format(testQueryTemplate, (int)(50 * sel)));
    testQueryFileNames.emplace_back(fmt::format("tpch-04-sel-{:.1f}", sel));
  }
  BitmapPushdownTestUtil::run_bitmap_pushdown_benchmark_query(cachingQuery,
                                                              testQueries,
                                                              testQueryFileNames,
                                                              false,
                                                              "0.01",
                                                              PARALLEL_FPDB_STORE_SAME_NODE,
                                                              true);
}

TEST_CASE ("bitmap-pushdown-test-benchmark-query-compute-bitmap-tpch-12" * doctest::skip(false || SKIP_SUITE)) {
  std::string cachingQuery = "select l_shipmode, l_commitdate, l_receiptdate, l_shipdate, l_quantity\n"
                             "from lineitem";
  std::string testQueryTemplate = "select\n"
                                  "  l.l_shipmode,\n"
                                  "  sum(case\n"
                                  "    when o.o_orderpriority = '1-URGENT'\n"
                                  "      or o.o_orderpriority = '2-HIGH'\n"
                                  "      then 1\n"
                                  "    else 0\n"
                                  "  end) as high_line_count,\n"
                                  "  sum(case\n"
                                  "    when o.o_orderpriority <> '1-URGENT'\n"
                                  "      and o.o_orderpriority <> '2-HIGH'\n"
                                  "      then 1\n"
                                  "    else 0\n"
                                  "  end) as low_line_count\n"
                                  "from\n"
                                  "  orders o,\n"
                                  "  lineitem l\n"
                                  "where\n"
                                  "  o.o_orderkey = l.l_orderkey\n"
                                  "  and o.o_orderdate >= date '1993-10-01'\n"
                                  "  and o.o_orderdate < date '1993-10-01' + interval '3' month"
                                  "  and l.l_quantity < {}\n"
                                  "  and l.l_shipmode >= '0'\n"
                                  "  and l.l_commitdate > date '1991-01-01'\n"
                                  "  and l.l_shipdate > date '1991-01-01'\n"
                                  "  and l.l_receiptdate > date '1991-01-01'\n"
                                  "group by\n"
                                  "  l.l_shipmode\n"
                                  "order by\n"
                                  "  l.l_shipmode";
  std::vector<std::string> testQueries;
  std::vector<std::string> testQueryFileNames;
  for (double sel = 0.0; sel < 1.0; sel += 0.1) {
    testQueries.emplace_back(fmt::format(testQueryTemplate, (int)(50 * sel)));
    testQueryFileNames.emplace_back(fmt::format("tpch-12-sel-{:.1f}", sel));
  }
  BitmapPushdownTestUtil::run_bitmap_pushdown_benchmark_query(cachingQuery,
                                                              testQueries,
                                                              testQueryFileNames,
                                                              false,
                                                              "0.01",
                                                              PARALLEL_FPDB_STORE_SAME_NODE,
                                                              true);
}

TEST_CASE ("bitmap-pushdown-test-benchmark-query-compute-bitmap-tpch-14" * doctest::skip(false || SKIP_SUITE)) {
  std::string cachingQuery = "select l_shipdate, l_quantity\n"
                             "from lineitem";
  std::string testQueryTemplate = "select\n"
                                  "  100.00 * sum(case\n"
                                  "    when p.p_type like 'PROMO%'\n"
                                  "      then l.l_extendedprice * (1 - l.l_discount)\n"
                                  "    else 0\n"
                                  "  end) / (sum(l.l_extendedprice * (1 - l.l_discount)) + 1) as promo_revenue\n"
                                  "from\n"
                                  "  lineitem l,\n"
                                  "  part p\n"
                                  "where\n"
                                  "  l.l_partkey = p.p_partkey\n"
                                  "  and p.p_brand = 'Brand#41'"
                                  "  and l.l_quantity < {}\n"
                                  "  and l.l_shipdate >= date '1991-01-01'\n"
                                  "  and l.l_shipdate < date '1999-01-01'";
  std::vector<std::string> testQueries;
  std::vector<std::string> testQueryFileNames;
  for (double sel = 0.0; sel < 1.0; sel += 0.1) {
    testQueries.emplace_back(fmt::format(testQueryTemplate, (int)(50 * sel)));
    testQueryFileNames.emplace_back(fmt::format("tpch-14-sel-{:.1f}", sel));
  }
  BitmapPushdownTestUtil::run_bitmap_pushdown_benchmark_query(cachingQuery,
                                                              testQueries,
                                                              testQueryFileNames,
                                                              false,
                                                              "0.01",
                                                              PARALLEL_FPDB_STORE_SAME_NODE,
                                                              true);
}

TEST_CASE ("bitmap-pushdown-test-benchmark-query-compute-bitmap-tpch-19" * doctest::skip(false || SKIP_SUITE)) {
  std::string cachingQuery = "select l_quantity, l_shipmode, l_shipinstruct\n"
                             "from lineitem";
  std::string testQueryTemplate = "select\n"
                                  "  sum(l.l_extendedprice* (1 - l.l_discount)) as revenue\n"
                                  "from\n"
                                  "  lineitem l,\n"
                                  "  part p\n"
                                  "where\n"
                                  "  (\n"
                                  "    p.p_partkey = l.l_partkey\n"
                                  "    and p.p_brand = 'Brand#41'\n"
                                  "    and p.p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')\n"
                                  "    and l.l_quantity >= 0 and l.l_quantity <= {0}\n"
                                  "    and p.p_size between 1 and 5\n"
                                  "    and l.l_shipmode >= '0'\n"
                                  "    and l.l_shipinstruct >= '0'\n"
                                  "  )\n"
                                  "  or\n"
                                  "  (\n"
                                  "    p.p_partkey = l.l_partkey\n"
                                  "    and p.p_brand = 'Brand#13'\n"
                                  "    and p.p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')\n"
                                  "    and l.l_quantity >= 0 and l.l_quantity <= {0}\n"
                                  "    and p.p_size between 6 and 10\n"
                                  "    and l.l_shipmode >= '0'\n"
                                  "    and l.l_shipinstruct >= '0'\n"
                                  "  )\n"
                                  "  or\n"
                                  "  (\n"
                                  "    p.p_partkey = l.l_partkey\n"
                                  "    and p.p_brand = 'Brand#55'\n"
                                  "    and p.p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')\n"
                                  "    and l.l_quantity >= 0 and l.l_quantity <= {0}\n"
                                  "    and p.p_size between 11 and 15\n"
                                  "    and l.l_shipmode >= '0'\n"
                                  "    and l.l_shipinstruct >= '0'\n"
                                  "  )";
  std::vector<std::string> testQueries;
  std::vector<std::string> testQueryFileNames;
  for (double sel = 0.0; sel < 1.0; sel += 0.1) {
    testQueries.emplace_back(fmt::format(testQueryTemplate, (int)(50 * sel)));
    testQueryFileNames.emplace_back(fmt::format("tpch-19-sel-{:.1f}", sel));
  }
  BitmapPushdownTestUtil::run_bitmap_pushdown_benchmark_query(cachingQuery,
                                                              testQueries,
                                                              testQueryFileNames,
                                                              false,
                                                              "0.01",
                                                              PARALLEL_FPDB_STORE_SAME_NODE,
                                                              true);
}

}

}

