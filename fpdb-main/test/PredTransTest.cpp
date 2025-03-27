//
// Created by Yifei Yang on 4/11/23.
//

#include "PredTransTestUtil.h"
#include "Globals.h"
#include <doctest/doctest.h>

/**
 * Predicate transfer test
 *
 * Single compute node, also as the single FPDB store node
 * Start Calcite server before running this
 *
 * For DSB tests, since the minimal size of data that we can generate is sf1 which is too large to directly put under
 * the code base, the user should prepare the `dsb-sf1/` data prior to running DSB tests
 */
namespace fpdb::main::test {

#define SKIP_SUITE false

// single-partition single-thread tests
TEST_SUITE ("pred-trans-tpch-sf0.01-single-part" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("pred-trans-tpch-sf0.01-single-part-01" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf0.01-single-part/parquet/", "tpch/original/01.sql", 1);
}

TEST_CASE ("pred-trans-tpch-sf0.01-single-part-02" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf0.01-single-part/parquet/", "tpch/original/02.sql", 1);
}

TEST_CASE ("pred-trans-tpch-sf0.01-single-part-03" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf0.01-single-part/parquet/", "tpch/original/03.sql", 1);
}

TEST_CASE ("pred-trans-tpch-sf0.01-single-part-04" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf0.01-single-part/parquet/", "tpch/original/04.sql", 1);
}

TEST_CASE ("pred-trans-tpch-sf0.01-single-part-05" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf0.01-single-part/parquet/", "tpch/original/05.sql", 1);
}

TEST_CASE ("pred-trans-tpch-sf0.01-single-part-06" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf0.01-single-part/parquet/", "tpch/original/06.sql", 1);
}

TEST_CASE ("pred-trans-tpch-sf0.01-single-part-07" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf0.01-single-part/parquet/", "tpch/original/07.sql", 1);
}

TEST_CASE ("pred-trans-tpch-sf0.01-single-part-08" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf0.01-single-part/parquet/", "tpch/original/08.sql", 1);
}

TEST_CASE ("pred-trans-tpch-sf0.01-single-part-09" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf0.01-single-part/parquet/", "tpch/original/09.sql", 1);
}

TEST_CASE ("pred-trans-tpch-sf0.01-single-part-10" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf0.01-single-part/parquet/", "tpch/original/10.sql", 1);
}

TEST_CASE ("pred-trans-tpch-sf0.01-single-part-11" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf0.01-single-part/parquet/", "tpch/original/11.sql", 1);
}

TEST_CASE ("pred-trans-tpch-sf0.01-single-part-12" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf0.01-single-part/parquet/", "tpch/original/12.sql", 1);
}

TEST_CASE ("pred-trans-tpch-sf0.01-single-part-13" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf0.01-single-part/parquet/", "tpch/original/13.sql", 1);
}

TEST_CASE ("pred-trans-tpch-sf0.01-single-part-14" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf0.01-single-part/parquet/", "tpch/original/14.sql", 1);
}

TEST_CASE ("pred-trans-tpch-sf0.01-single-part-15" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf0.01-single-part/parquet/", "tpch/original/15.sql", 1);
}

TEST_CASE ("pred-trans-tpch-sf0.01-single-part-16" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf0.01-single-part/parquet/", "tpch/original/16.sql", 1);
}

TEST_CASE ("pred-trans-tpch-sf0.01-single-part-17" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf0.01-single-part/parquet/", "tpch/original/17.sql", 1);
}

TEST_CASE ("pred-trans-tpch-sf0.01-single-part-18" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf0.01-single-part/parquet/", "tpch/original/18.sql", 1);
}

TEST_CASE ("pred-trans-tpch-sf0.01-single-part-19" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf0.01-single-part/parquet/", "tpch/original/19.sql", 1);
}

TEST_CASE ("pred-trans-tpch-sf0.01-single-part-20" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf0.01-single-part/parquet/", "tpch/original/20.sql", 1);
}

TEST_CASE ("pred-trans-tpch-sf0.01-single-part-21" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf0.01-single-part/parquet/", "tpch/original/21.sql", 1);
}

TEST_CASE ("pred-trans-tpch-sf0.01-single-part-22" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf0.01-single-part/parquet/", "tpch/original/22.sql", 1);
}

}

// single-partition single-thread tests
TEST_SUITE ("yannakakis-tpch-sf0.01-single-part" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("yannakakis-tpch-sf0.01-single-part-01" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf0.01-single-part/parquet/", "tpch/original/01.sql", 1, true, true);
}

TEST_CASE ("yannakakis-tpch-sf0.01-single-part-02" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf0.01-single-part/parquet/", "tpch/original/02.sql", 1, true, true);
}

TEST_CASE ("yannakakis-tpch-sf0.01-single-part-03" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf0.01-single-part/parquet/", "tpch/original/03.sql", 1, true, true);
}

TEST_CASE ("yannakakis-tpch-sf0.01-single-part-04" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf0.01-single-part/parquet/", "tpch/original/04.sql", 1, true, true);
}

TEST_CASE ("yannakakis-tpch-sf0.01-single-part-05" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf0.01-single-part/parquet/", "tpch/original/05.sql", 1, true, true);
}

TEST_CASE ("yannakakis-tpch-sf0.01-single-part-06" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf0.01-single-part/parquet/", "tpch/original/06.sql", 1, true, true);
}

TEST_CASE ("yannakakis-tpch-sf0.01-single-part-07" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf0.01-single-part/parquet/", "tpch/original/07.sql", 1, true, true);
}

TEST_CASE ("yannakakis-tpch-sf0.01-single-part-08" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf0.01-single-part/parquet/", "tpch/original/08.sql", 1, true, true);
}

TEST_CASE ("yannakakis-tpch-sf0.01-single-part-09" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf0.01-single-part/parquet/", "tpch/original/09.sql", 1, true, true);
}

TEST_CASE ("yannakakis-tpch-sf0.01-single-part-10" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf0.01-single-part/parquet/", "tpch/original/10.sql", 1, true, true);
}

TEST_CASE ("yannakakis-tpch-sf0.01-single-part-11" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf0.01-single-part/parquet/", "tpch/original/11.sql", 1, true, true);
}

TEST_CASE ("yannakakis-tpch-sf0.01-single-part-12" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf0.01-single-part/parquet/", "tpch/original/12.sql", 1, true, true);
}

TEST_CASE ("yannakakis-tpch-sf0.01-single-part-13" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf0.01-single-part/parquet/", "tpch/original/13.sql", 1, true, true);
}

TEST_CASE ("yannakakis-tpch-sf0.01-single-part-14" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf0.01-single-part/parquet/", "tpch/original/14.sql", 1, true, true);
}

TEST_CASE ("yannakakis-tpch-sf0.01-single-part-15" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf0.01-single-part/parquet/", "tpch/original/15.sql", 1, true, true);
}

TEST_CASE ("yannakakis-tpch-sf0.01-single-part-16" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf0.01-single-part/parquet/", "tpch/original/16.sql", 1, true, true);
}

TEST_CASE ("yannakakis-tpch-sf0.01-single-part-17" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf0.01-single-part/parquet/", "tpch/original/17.sql", 1, true, true);
}

TEST_CASE ("yannakakis-tpch-sf0.01-single-part-18" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf0.01-single-part/parquet/", "tpch/original/18.sql", 1, true, true);
}

TEST_CASE ("yannakakis-tpch-sf0.01-single-part-19" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf0.01-single-part/parquet/", "tpch/original/19.sql", 1, true, true);
}

TEST_CASE ("yannakakis-tpch-sf0.01-single-part-20" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf0.01-single-part/parquet/", "tpch/original/20.sql", 1, true, true);
}

TEST_CASE ("yannakakis-tpch-sf0.01-single-part-21" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf0.01-single-part/parquet/", "tpch/original/21.sql", 1, true, true);
}

TEST_CASE ("yannakakis-tpch-sf0.01-single-part-22" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf0.01-single-part/parquet/", "tpch/original/22.sql", 1, true, true);
}

}

// parallel tests
TEST_SUITE ("pred-trans-tpch-sf0.01-parallel" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("pred-trans-tpch-sf0.01-parallel-01" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf0.01/parquet/", "tpch/original/01.sql", PARALLEL_FPDB_STORE_SAME_NODE);
}

TEST_CASE ("pred-trans-tpch-sf0.01-parallel-02" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf0.01/parquet/", "tpch/original/02.sql", PARALLEL_FPDB_STORE_SAME_NODE);
}

TEST_CASE ("pred-trans-tpch-sf0.01-parallel-03" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf0.01/parquet/", "tpch/original/03.sql", PARALLEL_FPDB_STORE_SAME_NODE);
}

TEST_CASE ("pred-trans-tpch-sf0.01-parallel-04" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf0.01/parquet/", "tpch/original/04.sql", PARALLEL_FPDB_STORE_SAME_NODE);
}

TEST_CASE ("pred-trans-tpch-sf0.01-parallel-05" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf0.01/parquet/", "tpch/original/05.sql", PARALLEL_FPDB_STORE_SAME_NODE);
}

TEST_CASE ("pred-trans-tpch-sf0.01-parallel-06" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf0.01/parquet/", "tpch/original/06.sql", PARALLEL_FPDB_STORE_SAME_NODE);
}

TEST_CASE ("pred-trans-tpch-sf0.01-parallel-07" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf0.01/parquet/", "tpch/original/07.sql", PARALLEL_FPDB_STORE_SAME_NODE);
}

TEST_CASE ("pred-trans-tpch-sf0.01-parallel-08" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf0.01/parquet/", "tpch/original/08.sql", PARALLEL_FPDB_STORE_SAME_NODE);
}

TEST_CASE ("pred-trans-tpch-sf0.01-parallel-09" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf0.01/parquet/", "tpch/original/09.sql", PARALLEL_FPDB_STORE_SAME_NODE);
}

TEST_CASE ("pred-trans-tpch-sf0.01-parallel-10" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf0.01/parquet/", "tpch/original/10.sql", PARALLEL_FPDB_STORE_SAME_NODE);
}

TEST_CASE ("pred-trans-tpch-sf0.01-parallel-11" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf0.01/parquet/", "tpch/original/11.sql", PARALLEL_FPDB_STORE_SAME_NODE);
}

TEST_CASE ("pred-trans-tpch-sf0.01-parallel-12" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf0.01/parquet/", "tpch/original/12.sql", PARALLEL_FPDB_STORE_SAME_NODE);
}

TEST_CASE ("pred-trans-tpch-sf0.01-parallel-13" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf0.01/parquet/", "tpch/original/13.sql", PARALLEL_FPDB_STORE_SAME_NODE);
}

TEST_CASE ("pred-trans-tpch-sf0.01-parallel-14" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf0.01/parquet/", "tpch/original/14.sql", PARALLEL_FPDB_STORE_SAME_NODE);
}

TEST_CASE ("pred-trans-tpch-sf0.01-parallel-15" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf0.01/parquet/", "tpch/original/15.sql", PARALLEL_FPDB_STORE_SAME_NODE);
}

TEST_CASE ("pred-trans-tpch-sf0.01-parallel-16" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf0.01/parquet/", "tpch/original/16.sql", PARALLEL_FPDB_STORE_SAME_NODE);
}

TEST_CASE ("pred-trans-tpch-sf0.01-parallel-17" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf0.01/parquet/", "tpch/original/17.sql", PARALLEL_FPDB_STORE_SAME_NODE);
}

TEST_CASE ("pred-trans-tpch-sf0.01-parallel-18" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf0.01/parquet/", "tpch/original/18.sql", PARALLEL_FPDB_STORE_SAME_NODE);
}

TEST_CASE ("pred-trans-tpch-sf0.01-parallel-19" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf0.01/parquet/", "tpch/original/19.sql", PARALLEL_FPDB_STORE_SAME_NODE);
}

TEST_CASE ("pred-trans-tpch-sf0.01-parallel-20" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf0.01/parquet/", "tpch/original/20.sql", PARALLEL_FPDB_STORE_SAME_NODE);
}

TEST_CASE ("pred-trans-tpch-sf0.01-parallel-21" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf0.01/parquet/", "tpch/original/21.sql", PARALLEL_FPDB_STORE_SAME_NODE);
}

TEST_CASE ("pred-trans-tpch-sf0.01-parallel-22" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf0.01/parquet/", "tpch/original/22.sql", PARALLEL_FPDB_STORE_SAME_NODE);
}

}

// parallel tests varying num threads used
TEST_SUITE ("pred-trans-tpch-sf0.01-parallel-vary-threads" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("pred-trans-tpch-sf0.01-parallel-vary-threads-03" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTransVaryThreads("tpch-sf0.01/parquet/", "tpch/original/03.sql",
                                              {1, 2, 4, 8});
}

TEST_CASE ("pred-trans-tpch-sf0.01-parallel-vary-threads-07" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTransVaryThreads("tpch-sf0.01/parquet/", "tpch/original/07.sql",
                                              {1, 2, 4, 8});
}

TEST_CASE ("pred-trans-tpch-sf0.01-parallel-vary-threads-10" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTransVaryThreads("tpch-sf0.01/parquet/", "tpch/original/10.sql",
                                              {1, 2, 4, 8});
}

TEST_CASE ("pred-trans-tpch-sf0.01-parallel-vary-threads-18" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTransVaryThreads("tpch-sf0.01/parquet/", "tpch/original/18.sql",
                                              {1, 2, 4, 8});
}

TEST_CASE ("pred-trans-tpch-sf0.01-parallel-vary-threads-20" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTransVaryThreads("tpch-sf0.01/parquet/", "tpch/original/20.sql",
                                              {1, 2, 4, 8});
}

}

// parallel tests for dsb
TEST_SUITE ("pred-trans-dsb-sf1-parallel-spj" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("pred-trans-dsb-sf1-parallel-spj-13" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("dsb-sf1/parquet/", "dsb/original/spj/q13_spj.sql", PARALLEL_FPDB_STORE_SAME_NODE);
}

TEST_CASE ("pred-trans-dsb-sf1-parallel-spj-18" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("dsb-sf1/parquet/", "dsb/original/spj/q18_spj.sql", PARALLEL_FPDB_STORE_SAME_NODE);
}

TEST_CASE ("pred-trans-dsb-sf1-parallel-spj-19" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("dsb-sf1/parquet/", "dsb/original/spj/q19_spj.sql", PARALLEL_FPDB_STORE_SAME_NODE);
}

TEST_CASE ("pred-trans-dsb-sf1-parallel-spj-25" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("dsb-sf1/parquet/", "dsb/original/spj/q25_spj.sql", PARALLEL_FPDB_STORE_SAME_NODE);
}

TEST_CASE ("pred-trans-dsb-sf1-parallel-spj-27" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("dsb-sf1/parquet/", "dsb/original/spj/q27_spj.sql", PARALLEL_FPDB_STORE_SAME_NODE);
}

TEST_CASE ("pred-trans-dsb-sf1-parallel-spj-40" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("dsb-sf1/parquet/", "dsb/original/spj/q40_spj.sql", PARALLEL_FPDB_STORE_SAME_NODE);
}

TEST_CASE ("pred-trans-dsb-sf1-parallel-spj-50" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("dsb-sf1/parquet/", "dsb/original/spj/q50_spj.sql", PARALLEL_FPDB_STORE_SAME_NODE);
}

TEST_CASE ("pred-trans-dsb-sf1-parallel-spj-72" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("dsb-sf1/parquet/", "dsb/original/spj/q72_spj.sql", PARALLEL_FPDB_STORE_SAME_NODE);
}

TEST_CASE ("pred-trans-dsb-sf1-parallel-spj-84" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("dsb-sf1/parquet/", "dsb/original/spj/q84_spj.sql", PARALLEL_FPDB_STORE_SAME_NODE);
}

TEST_CASE ("pred-trans-dsb-sf1-parallel-spj-85" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("dsb-sf1/parquet/", "dsb/original/spj/q85_spj.sql", PARALLEL_FPDB_STORE_SAME_NODE);
}

TEST_CASE ("pred-trans-dsb-sf1-parallel-spj-91" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("dsb-sf1/parquet/", "dsb/original/spj/q91_spj.sql", PARALLEL_FPDB_STORE_SAME_NODE);
}

TEST_CASE ("pred-trans-dsb-sf1-parallel-spj-99" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("dsb-sf1/parquet/", "dsb/original/spj/q99_spj.sql", PARALLEL_FPDB_STORE_SAME_NODE);
}

TEST_CASE ("pred-trans-dsb-sf1-parallel-spj-100" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("dsb-sf1/parquet/", "dsb/original/spj/q100_spj.sql", PARALLEL_FPDB_STORE_SAME_NODE);
}

TEST_CASE ("pred-trans-dsb-sf1-parallel-spj-101" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("dsb-sf1/parquet/", "dsb/original/spj/q101_spj.sql", PARALLEL_FPDB_STORE_SAME_NODE,
                                   true, false, false);
}

TEST_CASE ("pred-trans-dsb-sf1-parallel-spj-102" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("dsb-sf1/parquet/", "dsb/original/spj/q102_spj.sql", PARALLEL_FPDB_STORE_SAME_NODE,
                                   true, false, false);
}

}

TEST_SUITE ("pred-trans-dsb-sf1-parallel-mb" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("pred-trans-dsb-sf1-parallel-mb-1" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("dsb-sf1/parquet/", "dsb/original/multi-block/q1.sql", PARALLEL_FPDB_STORE_SAME_NODE);
}

TEST_CASE ("pred-trans-dsb-sf1-parallel-mb-10" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("dsb-sf1/parquet/", "dsb/original/multi-block/q10.sql", PARALLEL_FPDB_STORE_SAME_NODE);
}

TEST_CASE ("pred-trans-dsb-sf1-parallel-mb-14" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("dsb-sf1/parquet/", "dsb/original/multi-block/q14.sql", PARALLEL_FPDB_STORE_SAME_NODE);
}

TEST_CASE ("pred-trans-dsb-sf1-parallel-mb-23" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("dsb-sf1/parquet/", "dsb/original/multi-block/q23.sql", PARALLEL_FPDB_STORE_SAME_NODE);
}

TEST_CASE ("pred-trans-dsb-sf1-parallel-mb-30" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("dsb-sf1/parquet/", "dsb/original/multi-block/q30.sql", PARALLEL_FPDB_STORE_SAME_NODE);
}

TEST_CASE ("pred-trans-dsb-sf1-parallel-mb-31" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("dsb-sf1/parquet/", "dsb/original/multi-block/q31.sql", PARALLEL_FPDB_STORE_SAME_NODE);
}

TEST_CASE ("pred-trans-dsb-sf1-parallel-mb-32" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("dsb-sf1/parquet/", "dsb/original/multi-block/q32.sql", PARALLEL_FPDB_STORE_SAME_NODE);
}

TEST_CASE ("pred-trans-dsb-sf1-parallel-mb-38" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("dsb-sf1/parquet/", "dsb/original/multi-block/q38.sql", PARALLEL_FPDB_STORE_SAME_NODE);
}

TEST_CASE ("pred-trans-dsb-sf1-parallel-mb-39-0" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("dsb-sf1/parquet/", "dsb/original/multi-block/q39_0.sql", PARALLEL_FPDB_STORE_SAME_NODE);
}

TEST_CASE ("pred-trans-dsb-sf1-parallel-mb-39-1" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("dsb-sf1/parquet/", "dsb/original/multi-block/q39_1.sql", PARALLEL_FPDB_STORE_SAME_NODE);
}

TEST_CASE ("pred-trans-dsb-sf1-parallel-mb-54" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("dsb-sf1/parquet/", "dsb/original/multi-block/q54.sql", PARALLEL_FPDB_STORE_SAME_NODE);
}

TEST_CASE ("pred-trans-dsb-sf1-parallel-mb-58" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("dsb-sf1/parquet/", "dsb/original/multi-block/q58.sql", PARALLEL_FPDB_STORE_SAME_NODE);
}

TEST_CASE ("pred-trans-dsb-sf1-parallel-mb-59" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("dsb-sf1/parquet/", "dsb/original/multi-block/q59.sql", PARALLEL_FPDB_STORE_SAME_NODE);
}

TEST_CASE ("pred-trans-dsb-sf1-parallel-mb-64" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("dsb-sf1/parquet/", "dsb/original/multi-block/q64.sql", PARALLEL_FPDB_STORE_SAME_NODE);
}

TEST_CASE ("pred-trans-dsb-sf1-parallel-mb-65" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("dsb-sf1/parquet/", "dsb/original/multi-block/q65.sql", PARALLEL_FPDB_STORE_SAME_NODE);
}

TEST_CASE ("pred-trans-dsb-sf1-parallel-mb-69" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("dsb-sf1/parquet/", "dsb/original/multi-block/q69.sql", PARALLEL_FPDB_STORE_SAME_NODE);
}

TEST_CASE ("pred-trans-dsb-sf1-parallel-mb-75" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("dsb-sf1/parquet/", "dsb/original/multi-block/q75.sql", PARALLEL_FPDB_STORE_SAME_NODE);
}

TEST_CASE ("pred-trans-dsb-sf1-parallel-mb-80" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("dsb-sf1/parquet/", "dsb/original/multi-block/q80.sql", PARALLEL_FPDB_STORE_SAME_NODE);
}

TEST_CASE ("pred-trans-dsb-sf1-parallel-mb-81" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("dsb-sf1/parquet/", "dsb/original/multi-block/q81.sql", PARALLEL_FPDB_STORE_SAME_NODE);
}

TEST_CASE ("pred-trans-dsb-sf1-parallel-mb-83" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("dsb-sf1/parquet/", "dsb/original/multi-block/q83.sql", PARALLEL_FPDB_STORE_SAME_NODE);
}

TEST_CASE ("pred-trans-dsb-sf1-parallel-mb-87" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("dsb-sf1/parquet/", "dsb/original/multi-block/q87.sql", PARALLEL_FPDB_STORE_SAME_NODE);
}

TEST_CASE ("pred-trans-dsb-sf1-parallel-mb-92" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("dsb-sf1/parquet/", "dsb/original/multi-block/q92.sql", PARALLEL_FPDB_STORE_SAME_NODE);
}

TEST_CASE ("pred-trans-dsb-sf1-parallel-mb-94" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("dsb-sf1/parquet/", "dsb/original/multi-block/q94.sql", PARALLEL_FPDB_STORE_SAME_NODE);
}

}

// tests for LIP
TEST_SUITE ("lip-tpch-sf0.01" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("lip-tpch-sf0.01-01" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testLIP("tpch-sf0.01/parquet/", "tpch/original/01.sql", PARALLEL_FPDB_STORE_SAME_NODE);
}

TEST_CASE ("lip-tpch-sf0.01-02" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testLIP("tpch-sf0.01/parquet/", "tpch/original/02.sql", PARALLEL_FPDB_STORE_SAME_NODE);
}

TEST_CASE ("lip-tpch-sf0.01-03" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testLIP("tpch-sf0.01/parquet/", "tpch/original/03.sql", PARALLEL_FPDB_STORE_SAME_NODE);
}

TEST_CASE ("lip-tpch-sf0.01-04" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testLIP("tpch-sf0.01/parquet/", "tpch/original/04.sql", PARALLEL_FPDB_STORE_SAME_NODE);
}

// force to use join order 2
TEST_CASE ("lip-tpch-sf0.01-05" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testLIP("tpch-sf0.01/parquet/", "tpch/modified/05-jo2.sql", PARALLEL_FPDB_STORE_SAME_NODE, false);
}

TEST_CASE ("lip-tpch-sf0.01-06" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testLIP("tpch-sf0.01/parquet/", "tpch/original/06.sql", PARALLEL_FPDB_STORE_SAME_NODE);
}

TEST_CASE ("lip-tpch-sf0.01-07" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testLIP("tpch-sf0.01/parquet/", "tpch/original/07.sql", PARALLEL_FPDB_STORE_SAME_NODE);
}

TEST_CASE ("lip-tpch-sf0.01-08" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testLIP("tpch-sf0.01/parquet/", "tpch/original/08.sql", PARALLEL_FPDB_STORE_SAME_NODE);
}

TEST_CASE ("lip-tpch-sf0.01-09" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testLIP("tpch-sf0.01/parquet/", "tpch/original/09.sql", PARALLEL_FPDB_STORE_SAME_NODE);
}

TEST_CASE ("lip-tpch-sf0.01-10" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testLIP("tpch-sf0.01/parquet/", "tpch/original/10.sql", PARALLEL_FPDB_STORE_SAME_NODE);
}

TEST_CASE ("lip-tpch-sf0.01-11" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testLIP("tpch-sf0.01/parquet/", "tpch/original/11.sql", PARALLEL_FPDB_STORE_SAME_NODE);
}

TEST_CASE ("lip-tpch-sf0.01-12" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testLIP("tpch-sf0.01/parquet/", "tpch/original/12.sql", PARALLEL_FPDB_STORE_SAME_NODE);
}

TEST_CASE ("lip-tpch-sf0.01-13" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testLIP("tpch-sf0.01/parquet/", "tpch/original/13.sql", PARALLEL_FPDB_STORE_SAME_NODE);
}

TEST_CASE ("lip-tpch-sf0.01-14" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testLIP("tpch-sf0.01/parquet/", "tpch/original/14.sql", PARALLEL_FPDB_STORE_SAME_NODE);
}

TEST_CASE ("lip-tpch-sf0.01-15" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testLIP("tpch-sf0.01/parquet/", "tpch/original/15.sql", PARALLEL_FPDB_STORE_SAME_NODE);
}

TEST_CASE ("lip-tpch-sf0.01-16" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testLIP("tpch-sf0.01/parquet/", "tpch/original/16.sql", PARALLEL_FPDB_STORE_SAME_NODE);
}

TEST_CASE ("lip-tpch-sf0.01-17" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testLIP("tpch-sf0.01/parquet/", "tpch/original/17.sql", PARALLEL_FPDB_STORE_SAME_NODE);
}

TEST_CASE ("lip-tpch-sf0.01-18" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testLIP("tpch-sf0.01/parquet/", "tpch/original/18.sql", PARALLEL_FPDB_STORE_SAME_NODE);
}

TEST_CASE ("lip-tpch-sf0.01-19" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testLIP("tpch-sf0.01/parquet/", "tpch/original/19.sql", PARALLEL_FPDB_STORE_SAME_NODE);
}

TEST_CASE ("lip-tpch-sf0.01-20" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testLIP("tpch-sf0.01/parquet/", "tpch/original/20.sql", PARALLEL_FPDB_STORE_SAME_NODE);
}

TEST_CASE ("lip-tpch-sf0.01-21" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testLIP("tpch-sf0.01/parquet/", "tpch/original/21.sql", PARALLEL_FPDB_STORE_SAME_NODE);
}

TEST_CASE ("lip-tpch-sf0.01-22" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testLIP("tpch-sf0.01/parquet/", "tpch/original/22.sql", PARALLEL_FPDB_STORE_SAME_NODE);
}

}

}
