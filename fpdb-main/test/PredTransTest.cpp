//
// Created by Yifei Yang on 4/11/23.
//

#include "PredTransTestUtil.h"
#include <doctest/doctest.h>

/**
 * Predicate transfer test
 *
 * Single compute node, also as the single FPDB store node
 * Start Calcite server before running this
 */
namespace fpdb::main::test {

#define SKIP_SUITE false

TEST_SUITE ("pred-trans-tpch-sf0.01-single-part" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("pred-trans-tpch-sf0.01-single-part-01" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf0.01-single-part/parquet/", "tpch/original/01.sql");
}

TEST_CASE ("pred-trans-tpch-sf0.01-single-part-02" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf0.01-single-part/parquet/", "tpch/original/02.sql");
}

TEST_CASE ("pred-trans-tpch-sf0.01-single-part-03" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf0.01-single-part/parquet/", "tpch/original/03.sql");
}

TEST_CASE ("pred-trans-tpch-sf0.01-single-part-04" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf0.01-single-part/parquet/", "tpch/original/04.sql");
}

TEST_CASE ("pred-trans-tpch-sf0.01-single-part-05" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf0.01-single-part/parquet/", "tpch/original/05.sql");
}

TEST_CASE ("pred-trans-tpch-sf0.01-single-part-06" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf0.01-single-part/parquet/", "tpch/original/06.sql");
}

TEST_CASE ("pred-trans-tpch-sf0.01-single-part-07" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf0.01-single-part/parquet/", "tpch/original/07.sql");
}

TEST_CASE ("pred-trans-tpch-sf0.01-single-part-08" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf0.01-single-part/parquet/", "tpch/original/08.sql");
}

TEST_CASE ("pred-trans-tpch-sf0.01-single-part-09" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf0.01-single-part/parquet/", "tpch/original/09.sql");
}

TEST_CASE ("pred-trans-tpch-sf0.01-single-part-10" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf0.01-single-part/parquet/", "tpch/original/10.sql");
}

TEST_CASE ("pred-trans-tpch-sf0.01-single-part-11" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf0.01-single-part/parquet/", "tpch/original/11.sql");
}

TEST_CASE ("pred-trans-tpch-sf0.01-single-part-12" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf0.01-single-part/parquet/", "tpch/original/12.sql");
}

TEST_CASE ("pred-trans-tpch-sf0.01-single-part-13" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf0.01-single-part/parquet/", "tpch/original/13.sql");
}

TEST_CASE ("pred-trans-tpch-sf0.01-single-part-14" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf0.01-single-part/parquet/", "tpch/original/14.sql");
}

TEST_CASE ("pred-trans-tpch-sf0.01-single-part-15" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf0.01-single-part/parquet/", "tpch/original/15.sql");
}

TEST_CASE ("pred-trans-tpch-sf0.01-single-part-16" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf0.01-single-part/parquet/", "tpch/original/16.sql");
}

TEST_CASE ("pred-trans-tpch-sf0.01-single-part-17" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf0.01-single-part/parquet/", "tpch/original/17.sql");
}

TEST_CASE ("pred-trans-tpch-sf0.01-single-part-18" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf0.01-single-part/parquet/", "tpch/original/18.sql");
}

TEST_CASE ("pred-trans-tpch-sf0.01-single-part-19" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf0.01-single-part/parquet/", "tpch/original/19.sql");
}

TEST_CASE ("pred-trans-tpch-sf0.01-single-part-20" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf0.01-single-part/parquet/", "tpch/original/20.sql");
}

TEST_CASE ("pred-trans-tpch-sf0.01-single-part-21" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf0.01-single-part/parquet/", "tpch/original/21.sql");
}

TEST_CASE ("pred-trans-tpch-sf0.01-single-part-22" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf0.01-single-part/parquet/", "tpch/original/22.sql");
}

}

TEST_SUITE ("yannakakis-tpch-sf0.01-single-part" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("yannakakis-tpch-sf0.01-single-part-01" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf0.01-single-part/parquet/", "tpch/original/01.sql", true, true);
}

TEST_CASE ("yannakakis-tpch-sf0.01-single-part-02" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf0.01-single-part/parquet/", "tpch/original/02.sql", true, true);
}

TEST_CASE ("yannakakis-tpch-sf0.01-single-part-03" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf0.01-single-part/parquet/", "tpch/original/03.sql", true, true);
}

TEST_CASE ("yannakakis-tpch-sf0.01-single-part-04" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf0.01-single-part/parquet/", "tpch/original/04.sql", true, true);
}

TEST_CASE ("yannakakis-tpch-sf0.01-single-part-05" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf0.01-single-part/parquet/", "tpch/original/05.sql", true, true);
}

TEST_CASE ("yannakakis-tpch-sf0.01-single-part-06" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf0.01-single-part/parquet/", "tpch/original/06.sql", true, true);
}

TEST_CASE ("yannakakis-tpch-sf0.01-single-part-07" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf0.01-single-part/parquet/", "tpch/original/07.sql", true, true);
}

TEST_CASE ("yannakakis-tpch-sf0.01-single-part-08" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf0.01-single-part/parquet/", "tpch/original/08.sql", true, true);
}

TEST_CASE ("yannakakis-tpch-sf0.01-single-part-09" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf0.01-single-part/parquet/", "tpch/original/09.sql", true, true);
}

TEST_CASE ("yannakakis-tpch-sf0.01-single-part-10" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf0.01-single-part/parquet/", "tpch/original/10.sql", true, true);
}

TEST_CASE ("yannakakis-tpch-sf0.01-single-part-11" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf0.01-single-part/parquet/", "tpch/original/11.sql", true, true);
}

TEST_CASE ("yannakakis-tpch-sf0.01-single-part-12" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf0.01-single-part/parquet/", "tpch/original/12.sql", true, true);
}

TEST_CASE ("yannakakis-tpch-sf0.01-single-part-13" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf0.01-single-part/parquet/", "tpch/original/13.sql", true, true);
}

TEST_CASE ("yannakakis-tpch-sf0.01-single-part-14" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf0.01-single-part/parquet/", "tpch/original/14.sql", true, true);
}

TEST_CASE ("yannakakis-tpch-sf0.01-single-part-15" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf0.01-single-part/parquet/", "tpch/original/15.sql", true, true);
}

TEST_CASE ("yannakakis-tpch-sf0.01-single-part-16" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf0.01-single-part/parquet/", "tpch/original/16.sql", true, true);
}

TEST_CASE ("yannakakis-tpch-sf0.01-single-part-17" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf0.01-single-part/parquet/", "tpch/original/17.sql", true, true);
}

TEST_CASE ("yannakakis-tpch-sf0.01-single-part-18" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf0.01-single-part/parquet/", "tpch/original/18.sql", true, true);
}

TEST_CASE ("yannakakis-tpch-sf0.01-single-part-19" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf0.01-single-part/parquet/", "tpch/original/19.sql", true, true);
}

TEST_CASE ("yannakakis-tpch-sf0.01-single-part-20" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf0.01-single-part/parquet/", "tpch/original/20.sql", true, true);
}

TEST_CASE ("yannakakis-tpch-sf0.01-single-part-21" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf0.01-single-part/parquet/", "tpch/original/21.sql", true, true);
}

TEST_CASE ("yannakakis-tpch-sf0.01-single-part-22" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf0.01-single-part/parquet/", "tpch/original/22.sql", true, true);
}

}

}
