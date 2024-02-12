//
// Created by Yifei Yang on 4/12/22.
//

#include <doctest/doctest.h>
#include "TestUtil.h"
#include "BitmapPushdownTestUtil.h"
#include "Globals.h"
#include <fpdb/executor/physical/Globals.h>
#include <fpdb/util/Util.h>
#include <cstdio>

using namespace fpdb::util;

namespace fpdb::main::test {

#define SKIP_SUITE false

TEST_SUITE ("bitmap-pushdown-bench" * doctest::skip(SKIP_SUITE)) {

int SF_BITMAP_PUSHDOWN_BENCH = 10;
double SELECTIVITY_BITMAP_PUSHDOWN_BENCH = 0.2;

TEST_CASE ("bitmap-pushdown-bench-tpch-fpdb-store-diff-node-compute-bitmap" * doctest::skip(false || SKIP_SUITE)) {
  std::vector<bool> enableBitMapPushdownConfigs = {true, false};
  std::string cachingQueryFileName = "caching.sql";
  std::string testQueryFileNameBase = "test_{}.sql";
  bool rerunFirst = false; // the first run is slower than the subsequent, so we rerun it

  // predicates with "and" that makes specified selectivity
  std::cout << fmt::format("Selectivity: {}", SELECTIVITY_BITMAP_PUSHDOWN_BENCH) << std::endl;
  std::string selectivityPred = fmt::format("l_discount < {}", 0.1 * SELECTIVITY_BITMAP_PUSHDOWN_BENCH);
  std::vector<std::string> allPredicates{selectivityPred,
                                         "l_quantity >= 0",
                                         "l_shipdate <= date '1998-12-31'",
                                         "l_commitdate <= date '1998-12-31'",
                                         "l_receiptdate <= date '1998-12-31'",
                                         "l_orderkey >= 0",
                                         "l_partkey >= 0"};

  for (bool enableBitMapPushdown: enableBitMapPushdownConfigs) {
    std::cout << fmt::format("Enable Bitmap Pushdown: {}", enableBitMapPushdown) << std::endl;
    fpdb::executor::physical::ENABLE_FILTER_BITMAP_PUSHDOWN = enableBitMapPushdown;

    for (uint predicateNum = 1; predicateNum <= allPredicates.size(); ++predicateNum) {
      // create queries
      std::vector<std::string> predicates(allPredicates.begin(), allPredicates.begin() + predicateNum);
      std::string cachingQuery = fmt::format("select\n"
                                             "    l_discount\n"
                                             "from\n"
                                             "    lineitem\n"
                                             "where\n"
                                             "    {}\n", fmt::join(predicates, " and "));
      std::string testQuery = fmt::format("select\n"
                                          "    l_tax, l_extendedprice\n"
                                          "from\n"
                                          "    lineitem\n"
                                          "where\n"
                                          "    {}\n", fmt::join(predicates, " and "));
      std::string testQueryFileName = fmt::format(testQueryFileNameBase, predicateNum);
      TestUtil::writeQueryToFile(cachingQueryFileName, cachingQuery);
      TestUtil::writeQueryToFile(testQueryFileName, testQuery);

      // run test
      TestUtil testUtil(fmt::format("tpch-sf{}/parquet/", SF_BITMAP_PUSHDOWN_BENCH),
                        {cachingQueryFileName,
                         testQueryFileName},
                        PARALLEL_FPDB_STORE_SAME_NODE,
                        false,
                        ObjStoreType::FPDB_STORE,
                        Mode::hybridMode(),
                        CachingPolicyType::LFU,
                        50L * 1024 * 1024 * 1024);
      // fix cache layout after caching query, otherwise it keeps fetching new segments
      // which is not intra-partition hybrid execution (i.e. hit data + loaded new cache data is enough for execution,
      // pushdown part actually does nothing)
      testUtil.setFixLayoutIndices({0});
              REQUIRE_NOTHROW(testUtil.runTest());

      // delete queries
      TestUtil::removeQueryFile(cachingQueryFileName);
      TestUtil::removeQueryFile(testQueryFileName);

      if (!rerunFirst) {
        --predicateNum;
        rerunFirst = true;
      }
    }
  }

  fpdb::executor::physical::ENABLE_FILTER_BITMAP_PUSHDOWN = false;
}

TEST_CASE ("bitmap-pushdown-bench-tpch-fpdb-store-diff-node-storage-bitmap" * doctest::skip(false || SKIP_SUITE)) {
  std::vector<bool> enableBitMapPushdownConfigs = {true, false};
  std::string cachingQueryFileName = "caching.sql";
  std::string testQueryFileNameBase = "test_{}.sql";
  bool rerunFirst = false; // the first run is slower than the subsequent, so we rerun it

  // predicate that makes specified selectivity
  std::cout << fmt::format("Selectivity: {}", SELECTIVITY_BITMAP_PUSHDOWN_BENCH) << std::endl;
  std::string selectivityPred = fmt::format("l_discount < {}", 0.1 * SELECTIVITY_BITMAP_PUSHDOWN_BENCH);
  std::vector<std::string> projectColumns{"l_returnflag",
                                          "l_linestatus",
                                          "l_quantity",
                                          "l_extendedprice",
                                          "l_tax",
                                          "l_shipdate",
                                          "l_commitdate"};

  for (bool enableBitMapPushdown: enableBitMapPushdownConfigs) {
    std::cout << fmt::format("Enable Bitmap Pushdown: {}", enableBitMapPushdown) << std::endl;
    fpdb::executor::physical::ENABLE_FILTER_BITMAP_PUSHDOWN = enableBitMapPushdown;

    for (uint projectColumnNum = 1; projectColumnNum <= projectColumns.size(); ++projectColumnNum) {
      // create queries, test query with ~20% selectivity
      std::vector<std::string> cachedProjectColumns(projectColumns.begin(), projectColumns.begin() + projectColumnNum);
      std::string cachingQuery = fmt::format("select\n"
                                             "    {}\n"
                                             "from\n"
                                             "    lineitem\n"
                                             "where\n"
                                             "    l_quantity < 20\n", fmt::join(cachedProjectColumns, ", "));
      std::string testQuery = fmt::format("select\n"
                                          "    {}\n"
                                          "from\n"
                                          "    lineitem\n"
                                          "where\n"
                                          "    {}\n", fmt::join(cachedProjectColumns, ", "), selectivityPred);
      std::string testQueryFileName = fmt::format(testQueryFileNameBase, projectColumnNum);
      TestUtil::writeQueryToFile(cachingQueryFileName, cachingQuery);
      TestUtil::writeQueryToFile(testQueryFileName, testQuery);

      // run test
      TestUtil testUtil(fmt::format("tpch-sf{}/parquet/", SF_BITMAP_PUSHDOWN_BENCH),
                        {cachingQueryFileName,
                         testQueryFileName},
                        PARALLEL_FPDB_STORE_SAME_NODE,
                        false,
                        ObjStoreType::FPDB_STORE,
                        Mode::hybridMode(),
                        CachingPolicyType::LFU,
                        50L * 1024 * 1024 * 1024);
      // fix cache layout after caching query, otherwise it keeps fetching new segments
      // which is not intra-partition hybrid execution (i.e. hit data + loaded new cache data is enough for execution,
      // pushdown part actually does nothing)
      testUtil.setFixLayoutIndices({0});
              REQUIRE_NOTHROW(testUtil.runTest());

      // delete queries
      TestUtil::removeQueryFile(cachingQueryFileName);
      TestUtil::removeQueryFile(testQueryFileName);

      if (!rerunFirst) {
        --projectColumnNum;
        rerunFirst = true;
      }
    }
  }

  fpdb::executor::physical::ENABLE_FILTER_BITMAP_PUSHDOWN = false;
}

}

TEST_SUITE ("bitmap-pushdown-bench-benchmark-query-storage-bitmap" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("bitmap-pushdown-bench-benchmark-query-storage-bitmap-ssb-1.1" * doctest::skip(false || SKIP_SUITE)) {
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
                                                              "50",
                                                              PARALLEL_FPDB_STORE_DIFF_NODE,
                                                              false);
}

TEST_CASE ("bitmap-pushdown-bench-benchmark-query-storage-bitmap-tpch-03" * doctest::skip(false || SKIP_SUITE)) {
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
                                                              "50",
                                                              PARALLEL_FPDB_STORE_DIFF_NODE,
                                                              false);
}

TEST_CASE ("bitmap-pushdown-bench-benchmark-query-storage-bitmap-tpch-04" * doctest::skip(false || SKIP_SUITE)) {
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
                                                              "50",
                                                              PARALLEL_FPDB_STORE_DIFF_NODE,
                                                              false);
}

TEST_CASE ("bitmap-pushdown-bench-benchmark-query-storage-bitmap-tpch-12" * doctest::skip(false || SKIP_SUITE)) {
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
                                                              "50",
                                                              PARALLEL_FPDB_STORE_DIFF_NODE,
                                                              false);
}

TEST_CASE ("bitmap-pushdown-bench-benchmark-query-storage-bitmap-tpch-14" * doctest::skip(false || SKIP_SUITE)) {
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
                                                              "50",
                                                              PARALLEL_FPDB_STORE_DIFF_NODE,
                                                              false);
}

TEST_CASE ("bitmap-pushdown-bench-benchmark-query-storage-bitmap-tpch-19" * doctest::skip(false || SKIP_SUITE)) {
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
                                                              "50",
                                                              PARALLEL_FPDB_STORE_DIFF_NODE,
                                                              false);
}

}

TEST_SUITE ("bitmap-pushdown-bench-benchmark-query-compute-bitmap" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("bitmap-pushdown-bench-benchmark-query-compute-bitmap-ssb-1.1" * doctest::skip(false || SKIP_SUITE)) {
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
                                                              "50",
                                                              PARALLEL_FPDB_STORE_DIFF_NODE,
                                                              false);
}

TEST_CASE ("bitmap-pushdown-bench-benchmark-query-compute-bitmap-tpch-03" * doctest::skip(false || SKIP_SUITE)) {
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
                                                              "50",
                                                              PARALLEL_FPDB_STORE_DIFF_NODE,
                                                              false);
}

TEST_CASE ("bitmap-pushdown-bench-benchmark-query-compute-bitmap-tpch-04" * doctest::skip(false || SKIP_SUITE)) {
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
                                                              "50",
                                                              PARALLEL_FPDB_STORE_DIFF_NODE,
                                                              false);
}

TEST_CASE ("bitmap-pushdown-bench-benchmark-query-compute-bitmap-tpch-12" * doctest::skip(false || SKIP_SUITE)) {
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
                                                              "50",
                                                              PARALLEL_FPDB_STORE_DIFF_NODE,
                                                              false);
}

TEST_CASE ("bitmap-pushdown-bench-benchmark-query-compute-bitmap-tpch-14" * doctest::skip(false || SKIP_SUITE)) {
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
                                                              "50",
                                                              PARALLEL_FPDB_STORE_DIFF_NODE,
                                                              false);
}

TEST_CASE ("bitmap-pushdown-bench-benchmark-query-compute-bitmap-tpch-19" * doctest::skip(false || SKIP_SUITE)) {
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
                                                              "50",
                                                              PARALLEL_FPDB_STORE_DIFF_NODE,
                                                              false);
}

}

}
