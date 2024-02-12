//
// Created by Yifei Yang on 5/3/23.
//

#include "PredTransTestUtil.h"
#include <doctest/doctest.h>

/**
 * Predicate transfer bench
 *
 * Single compute node, also as the single FPDB store node
 * Need to download data "tpch-sf1/parquet" and put under "test-resources/fpdb-store-0/flexpushdowndb/"
 * Start Calcite server before running this
 */
namespace fpdb::main::test {

#define SKIP_SUITE false

TEST_SUITE ("pred-trans-tpch-sf1" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("pred-trans-tpch-sf1-01" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/01.sql");
}

TEST_CASE ("pred-trans-tpch-sf1-02" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/02.sql");
}

TEST_CASE ("pred-trans-tpch-sf1-03" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/03.sql");
}

TEST_CASE ("pred-trans-tpch-sf1-04" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/04.sql");
}

TEST_CASE ("pred-trans-tpch-sf1-05" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/05.sql");
}

TEST_CASE ("pred-trans-tpch-sf1-06" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/06.sql");
}

TEST_CASE ("pred-trans-tpch-sf1-07" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/07.sql");
}

TEST_CASE ("pred-trans-tpch-sf1-08" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/08.sql");
}

TEST_CASE ("pred-trans-tpch-sf1-09" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/09.sql");
}

TEST_CASE ("pred-trans-tpch-sf1-10" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/10.sql");
}

TEST_CASE ("pred-trans-tpch-sf1-11" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/11.sql");
}

TEST_CASE ("pred-trans-tpch-sf1-12" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/12.sql");
}

TEST_CASE ("pred-trans-tpch-sf1-13" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/13.sql");
}

TEST_CASE ("pred-trans-tpch-sf1-14" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/14.sql");
}

TEST_CASE ("pred-trans-tpch-sf1-15" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/15.sql");
}

TEST_CASE ("pred-trans-tpch-sf1-16" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/16.sql");
}

TEST_CASE ("pred-trans-tpch-sf1-17" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/17.sql");
}

TEST_CASE ("pred-trans-tpch-sf1-18" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/18.sql");
}

TEST_CASE ("pred-trans-tpch-sf1-19" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/19.sql");
}

TEST_CASE ("pred-trans-tpch-sf1-20" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/20.sql");
}

TEST_CASE ("pred-trans-tpch-sf1-21" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/21.sql");
}

TEST_CASE ("pred-trans-tpch-sf1-22" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/22.sql");
}

// Try different join orders
TEST_CASE ("pred-trans-tpch-sf1-05-jo1" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/modified/05-jo1.sql", true, false, false);
}

TEST_CASE ("pred-trans-tpch-sf1-05-jo2" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/modified/05-jo2.sql", true, false, false);
}

TEST_CASE ("pred-trans-tpch-sf1-09-jo1" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/modified/09-jo1.sql", true, false, false);
}

TEST_CASE ("pred-trans-tpch-sf1-09-jo2" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/modified/09-jo2.sql", true, false, false);
}

}

TEST_SUITE ("no-pred-trans-tpch-sf1" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("no-pred-trans-tpch-sf1-01" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/01.sql", false);
}

TEST_CASE ("no-pred-trans-tpch-sf1-02" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/02.sql", false);
}

TEST_CASE ("no-pred-trans-tpch-sf1-03" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/03.sql", false);
}

TEST_CASE ("no-pred-trans-tpch-sf1-04" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/04.sql", false);
}

TEST_CASE ("no-pred-trans-tpch-sf1-05" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/05.sql", false);
}

TEST_CASE ("no-pred-trans-tpch-sf1-06" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/06.sql", false);
}

TEST_CASE ("no-pred-trans-tpch-sf1-07" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/07.sql", false);
}

TEST_CASE ("no-pred-trans-tpch-sf1-08" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/08.sql", false);
}

TEST_CASE ("no-pred-trans-tpch-sf1-09" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/09.sql", false);
}

TEST_CASE ("no-pred-trans-tpch-sf1-10" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/10.sql", false);
}

TEST_CASE ("no-pred-trans-tpch-sf1-11" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/11.sql", false);
}

TEST_CASE ("no-pred-trans-tpch-sf1-12" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/12.sql", false);
}

TEST_CASE ("no-pred-trans-tpch-sf1-13" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/13.sql", false);
}

TEST_CASE ("no-pred-trans-tpch-sf1-14" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/14.sql", false);
}

TEST_CASE ("no-pred-trans-tpch-sf1-15" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/15.sql", false);
}

TEST_CASE ("no-pred-trans-tpch-sf1-16" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/16.sql", false);
}

TEST_CASE ("no-pred-trans-tpch-sf1-17" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/17.sql", false);
}

TEST_CASE ("no-pred-trans-tpch-sf1-18" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/18.sql", false);
}

TEST_CASE ("no-pred-trans-tpch-sf1-19" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/19.sql", false);
}

TEST_CASE ("no-pred-trans-tpch-sf1-20" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/20.sql", false);
}

TEST_CASE ("no-pred-trans-tpch-sf1-21" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/21.sql", false);
}

TEST_CASE ("no-pred-trans-tpch-sf1-22" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/22.sql", false);
}

// Try different join orders
TEST_CASE ("no-pred-trans-tpch-sf1-05-jo1" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/modified/05-jo1.sql", false, false, false);
}

TEST_CASE ("no-pred-trans-tpch-sf1-05-jo2" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/modified/05-jo2.sql", false, false, false);
}

TEST_CASE ("no-pred-trans-tpch-sf1-09-jo1" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/modified/09-jo1.sql", false, false, false);
}

TEST_CASE ("no-pred-trans-tpch-sf1-09-jo2" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/modified/09-jo2.sql", false, false, false);
}

}

TEST_SUITE ("yannakakis-tpch-sf1" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("yannakakis-tpch-sf1-01" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/01.sql", true, true);
}

TEST_CASE ("yannakakis-tpch-sf1-02" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/02.sql", true, true);
}

TEST_CASE ("yannakakis-tpch-sf1-03" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/03.sql", true, true);
}

TEST_CASE ("yannakakis-tpch-sf1-04" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/04.sql", true, true);
}

TEST_CASE ("yannakakis-tpch-sf1-05" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/05.sql", true, true);
}

TEST_CASE ("yannakakis-tpch-sf1-06" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/06.sql", true, true);
}

TEST_CASE ("yannakakis-tpch-sf1-07" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/07.sql", true, true);
}

TEST_CASE ("yannakakis-tpch-sf1-08" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/08.sql", true, true);
}

TEST_CASE ("yannakakis-tpch-sf1-09" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/09.sql", true, true);
}

TEST_CASE ("yannakakis-tpch-sf1-10" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/10.sql", true, true);
}

TEST_CASE ("yannakakis-tpch-sf1-11" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/11.sql", true, true);
}

TEST_CASE ("yannakakis-tpch-sf1-12" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/12.sql", true, true);
}

TEST_CASE ("yannakakis-tpch-sf1-13" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/13.sql", true, true);
}

TEST_CASE ("yannakakis-tpch-sf1-14" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/14.sql", true, true);
}

TEST_CASE ("yannakakis-tpch-sf1-15" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/15.sql", true, true);
}

TEST_CASE ("yannakakis-tpch-sf1-16" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/16.sql", true, true);
}

TEST_CASE ("yannakakis-tpch-sf1-17" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/17.sql", true, true);
}

TEST_CASE ("yannakakis-tpch-sf1-18" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/18.sql", true, true);
}

TEST_CASE ("yannakakis-tpch-sf1-19" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/19.sql", true, true);
}

TEST_CASE ("yannakakis-tpch-sf1-20" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/20.sql", true, true);
}

TEST_CASE ("yannakakis-tpch-sf1-21" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/21.sql", true, true);
}

TEST_CASE ("yannakakis-tpch-sf1-22" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/22.sql", true, true);
}

// Try different join orders
TEST_CASE ("yannakakis-tpch-sf1-05-jo1" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/modified/05-jo1.sql", true, true, false);
}

TEST_CASE ("yannakakis-tpch-sf1-05-jo2" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/modified/05-jo2.sql", true, true, false);
}

TEST_CASE ("yannakakis-tpch-sf1-09-jo1" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/modified/09-jo1.sql", true, true, false);
}

TEST_CASE ("yannakakis-tpch-sf1-09-jo2" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/modified/09-jo2.sql", true, true, false);
}

}

}
