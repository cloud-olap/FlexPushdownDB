//
// Created by Yifei Yang on 11/2/23.
//

#include "PredTransTestUtil.h"
#include "Globals.h"
#include <doctest/doctest.h>

/**
 * Predicate transfer distributed test
 *
 * Multiple compute nodes, read data from S3
 * Start Calcite server on the coordinator (locally) and CAF server on all executors (remote nodes) before running this
 * (see README for more details)
 */
namespace fpdb::main::test {

#define SKIP_SUITE false

TEST_SUITE ("pred-trans-tpch-sf0.01-dist" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("pred-trans-tpch-sf0.01-dist-01" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTransDist("tpch-sf0.01/parquet/", "tpch/original/01.sql", PARALLEL_DIST_SF0_01);
}

TEST_CASE ("pred-trans-tpch-sf0.01-dist-02" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTransDist("tpch-sf0.01/parquet/", "tpch/original/02.sql", PARALLEL_DIST_SF0_01);
}

TEST_CASE ("pred-trans-tpch-sf0.01-dist-03" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTransDist("tpch-sf0.01/parquet/", "tpch/original/03.sql", PARALLEL_DIST_SF0_01);
}

TEST_CASE ("pred-trans-tpch-sf0.01-dist-04" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTransDist("tpch-sf0.01/parquet/", "tpch/original/04.sql", PARALLEL_DIST_SF0_01);
}

TEST_CASE ("pred-trans-tpch-sf0.01-dist-05" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTransDist("tpch-sf0.01/parquet/", "tpch/original/05.sql", PARALLEL_DIST_SF0_01);
}

TEST_CASE ("pred-trans-tpch-sf0.01-dist-06" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTransDist("tpch-sf0.01/parquet/", "tpch/original/06.sql", PARALLEL_DIST_SF0_01);
}

TEST_CASE ("pred-trans-tpch-sf0.01-dist-07" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTransDist("tpch-sf0.01/parquet/", "tpch/original/07.sql", PARALLEL_DIST_SF0_01);
}

TEST_CASE ("pred-trans-tpch-sf0.01-dist-08" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTransDist("tpch-sf0.01/parquet/", "tpch/original/08.sql", PARALLEL_DIST_SF0_01);
}

TEST_CASE ("pred-trans-tpch-sf0.01-dist-09" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTransDist("tpch-sf0.01/parquet/", "tpch/original/09.sql", PARALLEL_DIST_SF0_01);
}

TEST_CASE ("pred-trans-tpch-sf0.01-dist-10" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTransDist("tpch-sf0.01/parquet/", "tpch/original/10.sql", PARALLEL_DIST_SF0_01);
}

TEST_CASE ("pred-trans-tpch-sf0.01-dist-11" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTransDist("tpch-sf0.01/parquet/", "tpch/original/11.sql", PARALLEL_DIST_SF0_01);
}

TEST_CASE ("pred-trans-tpch-sf0.01-dist-12" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTransDist("tpch-sf0.01/parquet/", "tpch/original/12.sql", PARALLEL_DIST_SF0_01);
}

TEST_CASE ("pred-trans-tpch-sf0.01-dist-13" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTransDist("tpch-sf0.01/parquet/", "tpch/original/13.sql", PARALLEL_DIST_SF0_01);
}

TEST_CASE ("pred-trans-tpch-sf0.01-dist-14" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTransDist("tpch-sf0.01/parquet/", "tpch/original/14.sql", PARALLEL_DIST_SF0_01);
}

TEST_CASE ("pred-trans-tpch-sf0.01-dist-15" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTransDist("tpch-sf0.01/parquet/", "tpch/original/15.sql", PARALLEL_DIST_SF0_01);
}

TEST_CASE ("pred-trans-tpch-sf0.01-dist-16" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTransDist("tpch-sf0.01/parquet/", "tpch/original/16.sql", PARALLEL_DIST_SF0_01);
}

TEST_CASE ("pred-trans-tpch-sf0.01-dist-17" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTransDist("tpch-sf0.01/parquet/", "tpch/original/17.sql", PARALLEL_DIST_SF0_01);
}

TEST_CASE ("pred-trans-tpch-sf0.01-dist-18" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTransDist("tpch-sf0.01/parquet/", "tpch/original/18.sql", PARALLEL_DIST_SF0_01);
}

TEST_CASE ("pred-trans-tpch-sf0.01-dist-19" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTransDist("tpch-sf0.01/parquet/", "tpch/original/19.sql", PARALLEL_DIST_SF0_01);
}

TEST_CASE ("pred-trans-tpch-sf0.01-dist-20" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTransDist("tpch-sf0.01/parquet/", "tpch/original/20.sql", PARALLEL_DIST_SF0_01);
}

TEST_CASE ("pred-trans-tpch-sf0.01-dist-21" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTransDist("tpch-sf0.01/parquet/", "tpch/original/21.sql", PARALLEL_DIST_SF0_01);
}

TEST_CASE ("pred-trans-tpch-sf0.01-dist-22" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTransDist("tpch-sf0.01/parquet/", "tpch/original/22.sql", PARALLEL_DIST_SF0_01);
}

}

TEST_SUITE ("no-pred-trans-tpch-sf0.01-dist" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("no-pred-trans-tpch-sf0.01-dist-01" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTransDist("tpch-sf0.01/parquet/", "tpch/original/01.sql", PARALLEL_DIST_SF0_01, false);
}

TEST_CASE ("no-pred-trans-tpch-sf0.01-dist-02" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTransDist("tpch-sf0.01/parquet/", "tpch/original/02.sql", PARALLEL_DIST_SF0_01, false);
}

TEST_CASE ("no-pred-trans-tpch-sf0.01-dist-03" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTransDist("tpch-sf0.01/parquet/", "tpch/original/03.sql", PARALLEL_DIST_SF0_01, false);
}

TEST_CASE ("no-pred-trans-tpch-sf0.01-dist-04" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTransDist("tpch-sf0.01/parquet/", "tpch/original/04.sql", PARALLEL_DIST_SF0_01, false);
}

TEST_CASE ("no-pred-trans-tpch-sf0.01-dist-05" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTransDist("tpch-sf0.01/parquet/", "tpch/original/05.sql", PARALLEL_DIST_SF0_01, false);
}

TEST_CASE ("no-pred-trans-tpch-sf0.01-dist-06" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTransDist("tpch-sf0.01/parquet/", "tpch/original/06.sql", PARALLEL_DIST_SF0_01, false);
}

TEST_CASE ("no-pred-trans-tpch-sf0.01-dist-07" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTransDist("tpch-sf0.01/parquet/", "tpch/original/07.sql", PARALLEL_DIST_SF0_01, false);
}

TEST_CASE ("no-pred-trans-tpch-sf0.01-dist-08" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTransDist("tpch-sf0.01/parquet/", "tpch/original/08.sql", PARALLEL_DIST_SF0_01, false);
}

TEST_CASE ("no-pred-trans-tpch-sf0.01-dist-09" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTransDist("tpch-sf0.01/parquet/", "tpch/original/09.sql", PARALLEL_DIST_SF0_01, false);
}

TEST_CASE ("no-pred-trans-tpch-sf0.01-dist-10" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTransDist("tpch-sf0.01/parquet/", "tpch/original/10.sql", PARALLEL_DIST_SF0_01, false);
}

TEST_CASE ("no-pred-trans-tpch-sf0.01-dist-11" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTransDist("tpch-sf0.01/parquet/", "tpch/original/11.sql", PARALLEL_DIST_SF0_01, false);
}

TEST_CASE ("no-pred-trans-tpch-sf0.01-dist-12" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTransDist("tpch-sf0.01/parquet/", "tpch/original/12.sql", PARALLEL_DIST_SF0_01, false);
}

TEST_CASE ("no-pred-trans-tpch-sf0.01-dist-13" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTransDist("tpch-sf0.01/parquet/", "tpch/original/13.sql", PARALLEL_DIST_SF0_01, false);
}

TEST_CASE ("no-pred-trans-tpch-sf0.01-dist-14" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTransDist("tpch-sf0.01/parquet/", "tpch/original/14.sql", PARALLEL_DIST_SF0_01, false);
}

TEST_CASE ("no-pred-trans-tpch-sf0.01-dist-15" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTransDist("tpch-sf0.01/parquet/", "tpch/original/15.sql", PARALLEL_DIST_SF0_01, false);
}

TEST_CASE ("no-pred-trans-tpch-sf0.01-dist-16" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTransDist("tpch-sf0.01/parquet/", "tpch/original/16.sql", PARALLEL_DIST_SF0_01, false);
}

TEST_CASE ("no-pred-trans-tpch-sf0.01-dist-17" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTransDist("tpch-sf0.01/parquet/", "tpch/original/17.sql", PARALLEL_DIST_SF0_01, false);
}

TEST_CASE ("no-pred-trans-tpch-sf0.01-dist-18" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTransDist("tpch-sf0.01/parquet/", "tpch/original/18.sql", PARALLEL_DIST_SF0_01, false);
}

TEST_CASE ("no-pred-trans-tpch-sf0.01-dist-19" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTransDist("tpch-sf0.01/parquet/", "tpch/original/19.sql", PARALLEL_DIST_SF0_01, false);
}

TEST_CASE ("no-pred-trans-tpch-sf0.01-dist-20" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTransDist("tpch-sf0.01/parquet/", "tpch/original/20.sql", PARALLEL_DIST_SF0_01, false);
}

TEST_CASE ("no-pred-trans-tpch-sf0.01-dist-21" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTransDist("tpch-sf0.01/parquet/", "tpch/original/21.sql", PARALLEL_DIST_SF0_01, false);
}

TEST_CASE ("no-pred-trans-tpch-sf0.01-dist-22" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTransDist("tpch-sf0.01/parquet/", "tpch/original/22.sql", PARALLEL_DIST_SF0_01, false);
}

}

}
