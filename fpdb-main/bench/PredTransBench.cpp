//
// Created by Yifei Yang on 5/3/23.
//

#include "PredTransTestUtil.h"
#include <doctest/doctest.h>

/**
 * Predicate transfer bench
 *
 * Single compute node, also as the single FPDB store node
 * Need to download corresponding data (e.g., "tpch-sf1/parquet", "tpch-sf10-single-part/parquet") and put under
 * "test-resources/fpdb-store-0/flexpushdowndb/"
 * Start Calcite server before running this
 */
namespace fpdb::main::test {

#define SKIP_SUITE false

// single-partition single-thread tests
TEST_SUITE ("pred-trans-tpch-sf1-single-part" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("pred-trans-tpch-sf1-single-part-01" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/01.sql", 1);
}

TEST_CASE ("pred-trans-tpch-sf1-single-part-02" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/02.sql", 1);
}

TEST_CASE ("pred-trans-tpch-sf1-single-part-03" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/03.sql", 1);
}

TEST_CASE ("pred-trans-tpch-sf1-single-part-04" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/04.sql", 1);
}

TEST_CASE ("pred-trans-tpch-sf1-single-part-05" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/05.sql", 1);
}

TEST_CASE ("pred-trans-tpch-sf1-single-part-06" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/06.sql", 1);
}

TEST_CASE ("pred-trans-tpch-sf1-single-part-07" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/07.sql", 1);
}

TEST_CASE ("pred-trans-tpch-sf1-single-part-08" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/08.sql", 1);
}

TEST_CASE ("pred-trans-tpch-sf1-single-part-09" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/09.sql", 1);
}

TEST_CASE ("pred-trans-tpch-sf1-single-part-10" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/10.sql", 1);
}

TEST_CASE ("pred-trans-tpch-sf1-single-part-11" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/11.sql", 1);
}

TEST_CASE ("pred-trans-tpch-sf1-single-part-12" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/12.sql", 1);
}

TEST_CASE ("pred-trans-tpch-sf1-single-part-13" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/13.sql", 1);
}

TEST_CASE ("pred-trans-tpch-sf1-single-part-14" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/14.sql", 1);
}

TEST_CASE ("pred-trans-tpch-sf1-single-part-15" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/15.sql", 1);
}

TEST_CASE ("pred-trans-tpch-sf1-single-part-16" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/16.sql", 1);
}

TEST_CASE ("pred-trans-tpch-sf1-single-part-17" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/17.sql", 1);
}

TEST_CASE ("pred-trans-tpch-sf1-single-part-18" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/18.sql", 1);
}

TEST_CASE ("pred-trans-tpch-sf1-single-part-19" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/19.sql", 1);
}

TEST_CASE ("pred-trans-tpch-sf1-single-part-20" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/20.sql", 1);
}

TEST_CASE ("pred-trans-tpch-sf1-single-part-21" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/21.sql", 1);
}

TEST_CASE ("pred-trans-tpch-sf1-single-part-22" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/22.sql", 1);
}

// Try different join orders
TEST_CASE ("pred-trans-tpch-sf1-single-part-05-jo1" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/modified/05-jo1.sql", 1, true, false, false);
}

TEST_CASE ("pred-trans-tpch-sf1-single-part-05-jo2" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/modified/05-jo2.sql", 1, true, false, false);
}

TEST_CASE ("pred-trans-tpch-sf1-single-part-05-jo3" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/modified/05-jo3.sql", 1, true, false, false);
}

TEST_CASE ("pred-trans-tpch-sf1-single-part-09-jo1" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/modified/09-jo1.sql", 1, true, false, false);
}

TEST_CASE ("pred-trans-tpch-sf1-single-part-09-jo2" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/modified/09-jo2.sql", 1, true, false, false);
}

}

// single-partition single-thread tests
TEST_SUITE ("no-pred-trans-tpch-sf1-single-part" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("no-pred-trans-tpch-sf1-single-part-01" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/01.sql", 1, false);
}

TEST_CASE ("no-pred-trans-tpch-sf1-single-part-02" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/02.sql", 1, false);
}

TEST_CASE ("no-pred-trans-tpch-sf1-single-part-03" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/03.sql", 1, false);
}

TEST_CASE ("no-pred-trans-tpch-sf1-single-part-04" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/04.sql", 1, false);
}

TEST_CASE ("no-pred-trans-tpch-sf1-single-part-05" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/05.sql", 1, false);
}

TEST_CASE ("no-pred-trans-tpch-sf1-single-part-06" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/06.sql", 1, false);
}

TEST_CASE ("no-pred-trans-tpch-sf1-single-part-07" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/07.sql", 1, false);
}

TEST_CASE ("no-pred-trans-tpch-sf1-single-part-08" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/08.sql", 1, false);
}

TEST_CASE ("no-pred-trans-tpch-sf1-single-part-09" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/09.sql", 1, false);
}

TEST_CASE ("no-pred-trans-tpch-sf1-single-part-10" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/10.sql", 1, false);
}

TEST_CASE ("no-pred-trans-tpch-sf1-single-part-11" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/11.sql", 1, false);
}

TEST_CASE ("no-pred-trans-tpch-sf1-single-part-12" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/12.sql", 1, false);
}

TEST_CASE ("no-pred-trans-tpch-sf1-single-part-13" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/13.sql", 1, false);
}

TEST_CASE ("no-pred-trans-tpch-sf1-single-part-14" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/14.sql", 1, false);
}

TEST_CASE ("no-pred-trans-tpch-sf1-single-part-15" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/15.sql", 1, false);
}

TEST_CASE ("no-pred-trans-tpch-sf1-single-part-16" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/16.sql", 1, false);
}

TEST_CASE ("no-pred-trans-tpch-sf1-single-part-17" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/17.sql", 1, false);
}

TEST_CASE ("no-pred-trans-tpch-sf1-single-part-18" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/18.sql", 1, false);
}

TEST_CASE ("no-pred-trans-tpch-sf1-single-part-19" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/19.sql", 1, false);
}

TEST_CASE ("no-pred-trans-tpch-sf1-single-part-20" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/20.sql", 1, false);
}

TEST_CASE ("no-pred-trans-tpch-sf1-single-part-21" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/21.sql", 1, false);
}

TEST_CASE ("no-pred-trans-tpch-sf1-single-part-22" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/22.sql", 1, false);
}

// Try different join orders
TEST_CASE ("no-pred-trans-tpch-sf1-single-part-05-jo1" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/modified/05-jo1.sql", 1, false, false, false);
}

TEST_CASE ("no-pred-trans-tpch-sf1-single-part-05-jo2" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/modified/05-jo2.sql", 1, false, false, false);
}

TEST_CASE ("no-pred-trans-tpch-sf1-single-part-05-jo3" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/modified/05-jo3.sql", 1, false, false, false);
}

TEST_CASE ("no-pred-trans-tpch-sf1-single-part-09-jo1" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/modified/09-jo1.sql", 1, false, false, false);
}

TEST_CASE ("no-pred-trans-tpch-sf1-single-part-09-jo2" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/modified/09-jo2.sql", 1, false, false, false);
}

}

// single-partition single-thread tests
TEST_SUITE ("yannakakis-tpch-sf1-single-part" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("yannakakis-tpch-sf1-single-part-01" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/01.sql", 1, true, true);
}

TEST_CASE ("yannakakis-tpch-sf1-single-part-02" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/02.sql", 1, true, true);
}

TEST_CASE ("yannakakis-tpch-sf1-single-part-03" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/03.sql", 1, true, true);
}

TEST_CASE ("yannakakis-tpch-sf1-single-part-04" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/04.sql", 1, true, true);
}

TEST_CASE ("yannakakis-tpch-sf1-single-part-05" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/05.sql", 1, true, true);
}

TEST_CASE ("yannakakis-tpch-sf1-single-part-06" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/06.sql", 1, true, true);
}

TEST_CASE ("yannakakis-tpch-sf1-single-part-07" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/07.sql", 1, true, true);
}

TEST_CASE ("yannakakis-tpch-sf1-single-part-08" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/08.sql", 1, true, true);
}

TEST_CASE ("yannakakis-tpch-sf1-single-part-09" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/09.sql", 1, true, true);
}

TEST_CASE ("yannakakis-tpch-sf1-single-part-10" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/10.sql", 1, true, true);
}

TEST_CASE ("yannakakis-tpch-sf1-single-part-11" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/11.sql", 1, true, true);
}

TEST_CASE ("yannakakis-tpch-sf1-single-part-12" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/12.sql", 1, true, true);
}

TEST_CASE ("yannakakis-tpch-sf1-single-part-13" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/13.sql", 1, true, true);
}

TEST_CASE ("yannakakis-tpch-sf1-single-part-14" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/14.sql", 1, true, true);
}

TEST_CASE ("yannakakis-tpch-sf1-single-part-15" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/15.sql", 1, true, true);
}

TEST_CASE ("yannakakis-tpch-sf1-single-part-16" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/16.sql", 1, true, true);
}

TEST_CASE ("yannakakis-tpch-sf1-single-part-17" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/17.sql", 1, true, true);
}

TEST_CASE ("yannakakis-tpch-sf1-single-part-18" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/18.sql", 1, true, true);
}

TEST_CASE ("yannakakis-tpch-sf1-single-part-19" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/19.sql", 1, true, true);
}

TEST_CASE ("yannakakis-tpch-sf1-single-part-20" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/20.sql", 1, true, true);
}

TEST_CASE ("yannakakis-tpch-sf1-single-part-21" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/21.sql", 1, true, true);
}

TEST_CASE ("yannakakis-tpch-sf1-single-part-22" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/original/22.sql", 1, true, true);
}

// Try different join orders
TEST_CASE ("yannakakis-tpch-sf1-single-part-05-jo1" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/modified/05-jo1.sql", 1, true, true, false);
}

TEST_CASE ("yannakakis-tpch-sf1-single-part-05-jo2" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/modified/05-jo2.sql", 1, true, true, false);
}

TEST_CASE ("yannakakis-tpch-sf1-single-part-05-jo3" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/modified/05-jo3.sql", 1, true, true, false);
}

TEST_CASE ("yannakakis-tpch-sf1-single-part-09-jo1" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/modified/09-jo1.sql", 1, true, true, false);
}

TEST_CASE ("yannakakis-tpch-sf1-single-part-09-jo2" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf1/parquet/", "tpch/modified/09-jo2.sql", 1, true, true, false);
}

}

// single-partition single-thread tests
TEST_SUITE ("pred-trans-tpch-sf10-single-part" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("pred-trans-tpch-sf10-single-part-01" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf10-single-part/parquet/", "tpch/original/01.sql", 1);
}

TEST_CASE ("pred-trans-tpch-sf10-single-part-02" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf10-single-part/parquet/", "tpch/original/02.sql", 1);
}

TEST_CASE ("pred-trans-tpch-sf10-single-part-03" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf10-single-part/parquet/", "tpch/original/03.sql", 1);
}

TEST_CASE ("pred-trans-tpch-sf10-single-part-04" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf10-single-part/parquet/", "tpch/original/04.sql", 1);
}

TEST_CASE ("pred-trans-tpch-sf10-single-part-05" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf10-single-part/parquet/", "tpch/original/05.sql", 1);
}

TEST_CASE ("pred-trans-tpch-sf10-single-part-06" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf10-single-part/parquet/", "tpch/original/06.sql", 1);
}

TEST_CASE ("pred-trans-tpch-sf10-single-part-07" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf10-single-part/parquet/", "tpch/original/07.sql", 1);
}

TEST_CASE ("pred-trans-tpch-sf10-single-part-08" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf10-single-part/parquet/", "tpch/original/08.sql", 1);
}

TEST_CASE ("pred-trans-tpch-sf10-single-part-09" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf10-single-part/parquet/", "tpch/original/09.sql", 1);
}

TEST_CASE ("pred-trans-tpch-sf10-single-part-10" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf10-single-part/parquet/", "tpch/original/10.sql", 1);
}

TEST_CASE ("pred-trans-tpch-sf10-single-part-11" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf10-single-part/parquet/", "tpch/original/11.sql", 1);
}

TEST_CASE ("pred-trans-tpch-sf10-single-part-12" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf10-single-part/parquet/", "tpch/original/12.sql", 1);
}

TEST_CASE ("pred-trans-tpch-sf10-single-part-13" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf10-single-part/parquet/", "tpch/original/13.sql", 1);
}

TEST_CASE ("pred-trans-tpch-sf10-single-part-14" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf10-single-part/parquet/", "tpch/original/14.sql", 1);
}

TEST_CASE ("pred-trans-tpch-sf10-single-part-15" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf10-single-part/parquet/", "tpch/original/15.sql", 1);
}

TEST_CASE ("pred-trans-tpch-sf10-single-part-16" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf10-single-part/parquet/", "tpch/original/16.sql", 1);
}

TEST_CASE ("pred-trans-tpch-sf10-single-part-17" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf10-single-part/parquet/", "tpch/original/17.sql", 1);
}

TEST_CASE ("pred-trans-tpch-sf10-single-part-18" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf10-single-part/parquet/", "tpch/original/18.sql", 1);
}

TEST_CASE ("pred-trans-tpch-sf10-single-part-19" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf10-single-part/parquet/", "tpch/original/19.sql", 1);
}

TEST_CASE ("pred-trans-tpch-sf10-single-part-20" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf10-single-part/parquet/", "tpch/original/20.sql", 1);
}

TEST_CASE ("pred-trans-tpch-sf10-single-part-21" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf10-single-part/parquet/", "tpch/original/21.sql", 1);
}

TEST_CASE ("pred-trans-tpch-sf10-single-part-22" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf10-single-part/parquet/", "tpch/original/22.sql", 1);
}

// Try different join orders
TEST_CASE ("pred-trans-tpch-sf10-single-part-05-jo1" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf10-single-part/parquet/", "tpch/modified/05-jo1.sql", 1, true, false, false);
}

TEST_CASE ("pred-trans-tpch-sf10-single-part-05-jo2" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf10-single-part/parquet/", "tpch/modified/05-jo2.sql", 1, true, false, false);
}

TEST_CASE ("pred-trans-tpch-sf10-single-part-05-jo3" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf10-single-part/parquet/", "tpch/modified/05-jo3.sql", 1, true, false, false);
}

TEST_CASE ("pred-trans-tpch-sf10-single-part-09-jo1" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf10-single-part/parquet/", "tpch/modified/09-jo1.sql", 1, true, false, false);
}

TEST_CASE ("pred-trans-tpch-sf10-single-part-09-jo2" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf10-single-part/parquet/", "tpch/modified/09-jo2.sql", 1, true, false, false);
}

}

// single-partition single-thread tests
TEST_SUITE ("no-pred-trans-tpch-sf10-single-part" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("no-pred-trans-tpch-sf10-single-part-01" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf10-single-part/parquet/", "tpch/original/01.sql", 1, false);
}

TEST_CASE ("no-pred-trans-tpch-sf10-single-part-02" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf10-single-part/parquet/", "tpch/original/02.sql", 1, false);
}

TEST_CASE ("no-pred-trans-tpch-sf10-single-part-03" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf10-single-part/parquet/", "tpch/original/03.sql", 1, false);
}

TEST_CASE ("no-pred-trans-tpch-sf10-single-part-04" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf10-single-part/parquet/", "tpch/original/04.sql", 1, false);
}

TEST_CASE ("no-pred-trans-tpch-sf10-single-part-05" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf10-single-part/parquet/", "tpch/original/05.sql", 1, false);
}

TEST_CASE ("no-pred-trans-tpch-sf10-single-part-06" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf10-single-part/parquet/", "tpch/original/06.sql", 1, false);
}

TEST_CASE ("no-pred-trans-tpch-sf10-single-part-07" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf10-single-part/parquet/", "tpch/original/07.sql", 1, false);
}

TEST_CASE ("no-pred-trans-tpch-sf10-single-part-08" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf10-single-part/parquet/", "tpch/original/08.sql", 1, false);
}

TEST_CASE ("no-pred-trans-tpch-sf10-single-part-09" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf10-single-part/parquet/", "tpch/original/09.sql", 1, false);
}

TEST_CASE ("no-pred-trans-tpch-sf10-single-part-10" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf10-single-part/parquet/", "tpch/original/10.sql", 1, false);
}

TEST_CASE ("no-pred-trans-tpch-sf10-single-part-11" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf10-single-part/parquet/", "tpch/original/11.sql", 1, false);
}

TEST_CASE ("no-pred-trans-tpch-sf10-single-part-12" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf10-single-part/parquet/", "tpch/original/12.sql", 1, false);
}

TEST_CASE ("no-pred-trans-tpch-sf10-single-part-13" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf10-single-part/parquet/", "tpch/original/13.sql", 1, false);
}

TEST_CASE ("no-pred-trans-tpch-sf10-single-part-14" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf10-single-part/parquet/", "tpch/original/14.sql", 1, false);
}

TEST_CASE ("no-pred-trans-tpch-sf10-single-part-15" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf10-single-part/parquet/", "tpch/original/15.sql", 1, false);
}

TEST_CASE ("no-pred-trans-tpch-sf10-single-part-16" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf10-single-part/parquet/", "tpch/original/16.sql", 1, false);
}

TEST_CASE ("no-pred-trans-tpch-sf10-single-part-17" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf10-single-part/parquet/", "tpch/original/17.sql", 1, false);
}

TEST_CASE ("no-pred-trans-tpch-sf10-single-part-18" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf10-single-part/parquet/", "tpch/original/18.sql", 1, false);
}

TEST_CASE ("no-pred-trans-tpch-sf10-single-part-19" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf10-single-part/parquet/", "tpch/original/19.sql", 1, false);
}

TEST_CASE ("no-pred-trans-tpch-sf10-single-part-20" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf10-single-part/parquet/", "tpch/original/20.sql", 1, false);
}

TEST_CASE ("no-pred-trans-tpch-sf10-single-part-21" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf10-single-part/parquet/", "tpch/original/21.sql", 1, false);
}

TEST_CASE ("no-pred-trans-tpch-sf10-single-part-22" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf10-single-part/parquet/", "tpch/original/22.sql", 1, false);
}

// Try different join orders
TEST_CASE ("no-pred-trans-tpch-sf10-single-part-05-jo1" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf10-single-part/parquet/", "tpch/modified/05-jo1.sql", 1, false, false, false);
}

TEST_CASE ("no-pred-trans-tpch-sf10-single-part-05-jo2" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf10-single-part/parquet/", "tpch/modified/05-jo2.sql", 1, false, false, false);
}

TEST_CASE ("no-pred-trans-tpch-sf10-single-part-05-jo3" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf10-single-part/parquet/", "tpch/modified/05-jo3.sql", 1, false, false, false);
}

TEST_CASE ("no-pred-trans-tpch-sf10-single-part-09-jo1" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf10-single-part/parquet/", "tpch/modified/09-jo1.sql", 1, false, false, false);
}

TEST_CASE ("no-pred-trans-tpch-sf10-single-part-09-jo2" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf10-single-part/parquet/", "tpch/modified/09-jo2.sql", 1, false, false, false);
}

}

// single-partition single-thread tests
TEST_SUITE ("yannakakis-tpch-sf10-single-part" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("yannakakis-tpch-sf10-single-part-01" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf10-single-part/parquet/", "tpch/original/01.sql", 1, true, true);
}

TEST_CASE ("yannakakis-tpch-sf10-single-part-02" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf10-single-part/parquet/", "tpch/original/02.sql", 1, true, true);
}

TEST_CASE ("yannakakis-tpch-sf10-single-part-03" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf10-single-part/parquet/", "tpch/original/03.sql", 1, true, true);
}

TEST_CASE ("yannakakis-tpch-sf10-single-part-04" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf10-single-part/parquet/", "tpch/original/04.sql", 1, true, true);
}

TEST_CASE ("yannakakis-tpch-sf10-single-part-05" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf10-single-part/parquet/", "tpch/original/05.sql", 1, true, true);
}

TEST_CASE ("yannakakis-tpch-sf10-single-part-06" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf10-single-part/parquet/", "tpch/original/06.sql", 1, true, true);
}

TEST_CASE ("yannakakis-tpch-sf10-single-part-07" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf10-single-part/parquet/", "tpch/original/07.sql", 1, true, true);
}

TEST_CASE ("yannakakis-tpch-sf10-single-part-08" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf10-single-part/parquet/", "tpch/original/08.sql", 1, true, true);
}

TEST_CASE ("yannakakis-tpch-sf10-single-part-09" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf10-single-part/parquet/", "tpch/original/09.sql", 1, true, true);
}

TEST_CASE ("yannakakis-tpch-sf10-single-part-10" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf10-single-part/parquet/", "tpch/original/10.sql", 1, true, true);
}

TEST_CASE ("yannakakis-tpch-sf10-single-part-11" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf10-single-part/parquet/", "tpch/original/11.sql", 1, true, true);
}

TEST_CASE ("yannakakis-tpch-sf10-single-part-12" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf10-single-part/parquet/", "tpch/original/12.sql", 1, true, true);
}

TEST_CASE ("yannakakis-tpch-sf10-single-part-13" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf10-single-part/parquet/", "tpch/original/13.sql", 1, true, true);
}

TEST_CASE ("yannakakis-tpch-sf10-single-part-14" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf10-single-part/parquet/", "tpch/original/14.sql", 1, true, true);
}

TEST_CASE ("yannakakis-tpch-sf10-single-part-15" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf10-single-part/parquet/", "tpch/original/15.sql", 1, true, true);
}

TEST_CASE ("yannakakis-tpch-sf10-single-part-16" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf10-single-part/parquet/", "tpch/original/16.sql", 1, true, true);
}

TEST_CASE ("yannakakis-tpch-sf10-single-part-17" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf10-single-part/parquet/", "tpch/original/17.sql", 1, true, true);
}

TEST_CASE ("yannakakis-tpch-sf10-single-part-18" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf10-single-part/parquet/", "tpch/original/18.sql", 1, true, true);
}

TEST_CASE ("yannakakis-tpch-sf10-single-part-19" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf10-single-part/parquet/", "tpch/original/19.sql", 1, true, true);
}

TEST_CASE ("yannakakis-tpch-sf10-single-part-20" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf10-single-part/parquet/", "tpch/original/20.sql", 1, true, true);
}

TEST_CASE ("yannakakis-tpch-sf10-single-part-21" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf10-single-part/parquet/", "tpch/original/21.sql", 1, true, true);
}

TEST_CASE ("yannakakis-tpch-sf10-single-part-22" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf10-single-part/parquet/", "tpch/original/22.sql", 1, true, true);
}

// Try different join orders
TEST_CASE ("yannakakis-tpch-sf10-single-part-05-jo1" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf10-single-part/parquet/", "tpch/modified/05-jo1.sql", 1, true, true, false);
}

TEST_CASE ("yannakakis-tpch-sf10-single-part-05-jo2" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf10-single-part/parquet/", "tpch/modified/05-jo2.sql", 1, true, true, false);
}

TEST_CASE ("yannakakis-tpch-sf10-single-part-05-jo3" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf10-single-part/parquet/", "tpch/modified/05-jo3.sql", 1, true, true, false);
}

TEST_CASE ("yannakakis-tpch-sf10-single-part-09-jo1" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf10-single-part/parquet/", "tpch/modified/09-jo1.sql", 1, true, true, false);
}

TEST_CASE ("yannakakis-tpch-sf10-single-part-09-jo2" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("tpch-sf10-single-part/parquet/", "tpch/modified/09-jo2.sql", 1, true, true, false);
}

}

// parallel tests varying num threads used
TEST_SUITE ("pred-trans-tpch-sf10-parallel-vary-threads" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("pred-trans-tpch-sf10-parallel-vary-threads-03" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTransVaryThreads("tpch-sf10/parquet/", "tpch/original/03.sql",
                                              PredTransTestUtil::genNumThreadsVec());
}

TEST_CASE ("pred-trans-tpch-sf10-parallel-vary-threads-07" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTransVaryThreads("tpch-sf10/parquet/", "tpch/original/07.sql",
                                              PredTransTestUtil::genNumThreadsVec());
}

TEST_CASE ("pred-trans-tpch-sf10-parallel-vary-threads-10" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTransVaryThreads("tpch-sf10/parquet/", "tpch/original/10.sql",
                                              PredTransTestUtil::genNumThreadsVec());
}

TEST_CASE ("pred-trans-tpch-sf10-parallel-vary-threads-18" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTransVaryThreads("tpch-sf10/parquet/", "tpch/original/18.sql",
                                              PredTransTestUtil::genNumThreadsVec());
}

TEST_CASE ("pred-trans-tpch-sf10-parallel-vary-threads-20" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTransVaryThreads("tpch-sf10/parquet/", "tpch/original/20.sql",
                                              PredTransTestUtil::genNumThreadsVec());
}

}

// parallel tests varying num threads used
TEST_SUITE ("pred-trans-tpch-sf100-parallel-vary-threads" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("pred-trans-tpch-sf100-parallel-vary-threads-01" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTransVaryThreads("tpch-sf100-4-node-hash-part/parquet/", "tpch/original/01.sql",
                                              PredTransTestUtil::genNumThreadsVec());
}

TEST_CASE ("pred-trans-tpch-sf100-parallel-vary-threads-02" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTransVaryThreads("tpch-sf100-4-node-hash-part/parquet/", "tpch/original/02.sql",
                                              PredTransTestUtil::genNumThreadsVec());
}

TEST_CASE ("pred-trans-tpch-sf100-parallel-vary-threads-03" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTransVaryThreads("tpch-sf100-4-node-hash-part/parquet/", "tpch/original/03.sql",
                                              PredTransTestUtil::genNumThreadsVec());
}

TEST_CASE ("pred-trans-tpch-sf100-parallel-vary-threads-04" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTransVaryThreads("tpch-sf100-4-node-hash-part/parquet/", "tpch/original/04.sql",
                                              PredTransTestUtil::genNumThreadsVec());
}

// force to use join order 2
TEST_CASE ("pred-trans-tpch-sf100-parallel-vary-threads-05" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTransVaryThreads("tpch-sf100-4-node-hash-part/parquet/", "tpch/modified/05-jo2.sql",
                                              PredTransTestUtil::genNumThreadsVec(), true, false);
}

TEST_CASE ("pred-trans-tpch-sf100-parallel-vary-threads-06" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTransVaryThreads("tpch-sf100-4-node-hash-part/parquet/", "tpch/original/06.sql",
                                              PredTransTestUtil::genNumThreadsVec());
}

TEST_CASE ("pred-trans-tpch-sf100-parallel-vary-threads-07" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTransVaryThreads("tpch-sf100-4-node-hash-part/parquet/", "tpch/original/07.sql",
                                              PredTransTestUtil::genNumThreadsVec());
}

TEST_CASE ("pred-trans-tpch-sf100-parallel-vary-threads-08" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTransVaryThreads("tpch-sf100-4-node-hash-part/parquet/", "tpch/original/08.sql",
                                              PredTransTestUtil::genNumThreadsVec());
}

TEST_CASE ("pred-trans-tpch-sf100-parallel-vary-threads-09" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTransVaryThreads("tpch-sf100-4-node-hash-part/parquet/", "tpch/original/09.sql",
                                              PredTransTestUtil::genNumThreadsVec());
}

// A single run can only finish threads 1-9, need another run to finish the part for 10-num threads.
TEST_CASE ("pred-trans-tpch-sf100-parallel-vary-threads-10" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTransVaryThreads("tpch-sf100-4-node-hash-part/parquet/", "tpch/original/10.sql",
                                              PredTransTestUtil::genNumThreadsVec());
}

TEST_CASE ("pred-trans-tpch-sf100-parallel-vary-threads-11" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTransVaryThreads("tpch-sf100-4-node-hash-part/parquet/", "tpch/original/11.sql",
                                              PredTransTestUtil::genNumThreadsVec());
}

TEST_CASE ("pred-trans-tpch-sf100-parallel-vary-threads-12" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTransVaryThreads("tpch-sf100-4-node-hash-part/parquet/", "tpch/original/12.sql",
                                              PredTransTestUtil::genNumThreadsVec());
}

TEST_CASE ("pred-trans-tpch-sf100-parallel-vary-threads-13" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTransVaryThreads("tpch-sf100-4-node-hash-part/parquet/", "tpch/original/13.sql",
                                              PredTransTestUtil::genNumThreadsVec());
}

TEST_CASE ("pred-trans-tpch-sf100-parallel-vary-threads-14" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTransVaryThreads("tpch-sf100-4-node-hash-part/parquet/", "tpch/original/14.sql",
                                              PredTransTestUtil::genNumThreadsVec());
}

TEST_CASE ("pred-trans-tpch-sf100-parallel-vary-threads-15" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTransVaryThreads("tpch-sf100-4-node-hash-part/parquet/", "tpch/original/15.sql",
                                              PredTransTestUtil::genNumThreadsVec());
}

TEST_CASE ("pred-trans-tpch-sf100-parallel-vary-threads-16" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTransVaryThreads("tpch-sf100-4-node-hash-part/parquet/", "tpch/original/16.sql",
                                              PredTransTestUtil::genNumThreadsVec());
}

TEST_CASE ("pred-trans-tpch-sf100-parallel-vary-threads-17" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTransVaryThreads("tpch-sf100-4-node-hash-part/parquet/", "tpch/original/17.sql",
                                              PredTransTestUtil::genNumThreadsVec());
}

TEST_CASE ("pred-trans-tpch-sf100-parallel-vary-threads-18" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTransVaryThreads("tpch-sf100-4-node-hash-part/parquet/", "tpch/original/18.sql",
                                              PredTransTestUtil::genNumThreadsVec());
}

TEST_CASE ("pred-trans-tpch-sf100-parallel-vary-threads-19" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTransVaryThreads("tpch-sf100-4-node-hash-part/parquet/", "tpch/original/19.sql",
                                              PredTransTestUtil::genNumThreadsVec());
}

TEST_CASE ("pred-trans-tpch-sf100-parallel-vary-threads-20" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTransVaryThreads("tpch-sf100-4-node-hash-part/parquet/", "tpch/original/20.sql",
                                              PredTransTestUtil::genNumThreadsVec());
}

// OOM for sf100, sf50 works
TEST_CASE ("pred-trans-tpch-sf100-parallel-vary-threads-21" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTransVaryThreads("tpch-sf100-4-node-hash-part/parquet/", "tpch/original/21.sql",
                                              PredTransTestUtil::genNumThreadsVec());
}

TEST_CASE ("pred-trans-tpch-sf100-parallel-vary-threads-22" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTransVaryThreads("tpch-sf100-4-node-hash-part/parquet/", "tpch/original/22.sql",
                                              PredTransTestUtil::genNumThreadsVec());
}

}

// parallel tests varying num threads used
TEST_SUITE ("no-pred-trans-tpch-sf100-parallel-vary-threads" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("no-pred-trans-tpch-sf100-parallel-vary-threads-01" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTransVaryThreads("tpch-sf100-4-node-hash-part/parquet/", "tpch/original/01.sql",
                                              PredTransTestUtil::genNumThreadsVec(), false);
}

TEST_CASE ("no-pred-trans-tpch-sf100-parallel-vary-threads-02" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTransVaryThreads("tpch-sf100-4-node-hash-part/parquet/", "tpch/original/02.sql",
                                              PredTransTestUtil::genNumThreadsVec(), false);
}

TEST_CASE ("no-pred-trans-tpch-sf100-parallel-vary-threads-03" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTransVaryThreads("tpch-sf100-4-node-hash-part/parquet/", "tpch/original/03.sql",
                                              PredTransTestUtil::genNumThreadsVec(3), false);
}

TEST_CASE ("no-pred-trans-tpch-sf100-parallel-vary-threads-04" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTransVaryThreads("tpch-sf100-4-node-hash-part/parquet/", "tpch/original/04.sql",
                                              PredTransTestUtil::genNumThreadsVec(), false);
}

// force to use join order 2
TEST_CASE ("no-pred-trans-tpch-sf100-parallel-vary-threads-05" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTransVaryThreads("tpch-sf100-4-node-hash-part/parquet/", "tpch/modified/05-jo2.sql",
                                              PredTransTestUtil::genNumThreadsVec(), false, false);
}

TEST_CASE ("no-pred-trans-tpch-sf100-parallel-vary-threads-06" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTransVaryThreads("tpch-sf100-4-node-hash-part/parquet/", "tpch/original/06.sql",
                                              PredTransTestUtil::genNumThreadsVec(), false);
}

TEST_CASE ("no-pred-trans-tpch-sf100-parallel-vary-threads-07" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTransVaryThreads("tpch-sf100-4-node-hash-part/parquet/", "tpch/original/07.sql",
                                              PredTransTestUtil::genNumThreadsVec(), false);
}

TEST_CASE ("no-pred-trans-tpch-sf100-parallel-vary-threads-08" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTransVaryThreads("tpch-sf100-4-node-hash-part/parquet/", "tpch/original/08.sql",
                                              PredTransTestUtil::genNumThreadsVec(), false);
}

// OOM for sf100, sf50 works
TEST_CASE ("no-pred-trans-tpch-sf100-parallel-vary-threads-09" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTransVaryThreads("tpch-sf100-4-node-hash-part/parquet/", "tpch/original/09.sql",
                                              PredTransTestUtil::genNumThreadsVec(), false);
}

TEST_CASE ("no-pred-trans-tpch-sf100-parallel-vary-threads-10" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTransVaryThreads("tpch-sf100-4-node-hash-part/parquet/", "tpch/original/10.sql",
                                              PredTransTestUtil::genNumThreadsVec(2), false);
}

TEST_CASE ("no-pred-trans-tpch-sf100-parallel-vary-threads-11" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTransVaryThreads("tpch-sf100-4-node-hash-part/parquet/", "tpch/original/11.sql",
                                              PredTransTestUtil::genNumThreadsVec(), false);
}

TEST_CASE ("no-pred-trans-tpch-sf100-parallel-vary-threads-12" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTransVaryThreads("tpch-sf100-4-node-hash-part/parquet/", "tpch/original/12.sql",
                                              PredTransTestUtil::genNumThreadsVec(), false);
}

TEST_CASE ("no-pred-trans-tpch-sf100-parallel-vary-threads-13" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTransVaryThreads("tpch-sf100-4-node-hash-part/parquet/", "tpch/original/13.sql",
                                              PredTransTestUtil::genNumThreadsVec(), false);
}

TEST_CASE ("no-pred-trans-tpch-sf100-parallel-vary-threads-14" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTransVaryThreads("tpch-sf100-4-node-hash-part/parquet/", "tpch/original/14.sql",
                                              PredTransTestUtil::genNumThreadsVec(), false);
}

TEST_CASE ("no-pred-trans-tpch-sf100-parallel-vary-threads-15" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTransVaryThreads("tpch-sf100-4-node-hash-part/parquet/", "tpch/original/15.sql",
                                              PredTransTestUtil::genNumThreadsVec(), false);
}

TEST_CASE ("no-pred-trans-tpch-sf100-parallel-vary-threads-16" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTransVaryThreads("tpch-sf100-4-node-hash-part/parquet/", "tpch/original/16.sql",
                                              PredTransTestUtil::genNumThreadsVec(), false);
}

TEST_CASE ("no-pred-trans-tpch-sf100-parallel-vary-threads-17" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTransVaryThreads("tpch-sf100-4-node-hash-part/parquet/", "tpch/original/17.sql",
                                              PredTransTestUtil::genNumThreadsVec(), false);
}

TEST_CASE ("no-pred-trans-tpch-sf100-parallel-vary-threads-18" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTransVaryThreads("tpch-sf100-4-node-hash-part/parquet/", "tpch/original/18.sql",
                                              PredTransTestUtil::genNumThreadsVec(), false);
}

TEST_CASE ("no-pred-trans-tpch-sf100-parallel-vary-threads-19" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTransVaryThreads("tpch-sf100-4-node-hash-part/parquet/", "tpch/original/19.sql",
                                              PredTransTestUtil::genNumThreadsVec(), false);
}

TEST_CASE ("no-pred-trans-tpch-sf100-parallel-vary-threads-20" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTransVaryThreads("tpch-sf100-4-node-hash-part/parquet/", "tpch/original/20.sql",
                                              PredTransTestUtil::genNumThreadsVec(), false);
}

// OOM for sf100 and sf50, sf10 works
TEST_CASE ("no-pred-trans-tpch-sf100-parallel-vary-threads-21" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTransVaryThreads("tpch-sf100-4-node-hash-part/parquet/", "tpch/original/21.sql",
                                              PredTransTestUtil::genNumThreadsVec(), false);
}

TEST_CASE ("no-pred-trans-tpch-sf100-parallel-vary-threads-22" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTransVaryThreads("tpch-sf100-4-node-hash-part/parquet/", "tpch/original/22.sql",
                                              PredTransTestUtil::genNumThreadsVec(), false);
}

}

// single-partition single thread tests
TEST_SUITE ("no-pred-trans-job-single-part" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("no-pred-trans-job-single-part-1a" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/1a.sql", 1, false);
}

TEST_CASE ("no-pred-trans-job-single-part-1b" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/1b.sql", 1, false);
}

TEST_CASE ("no-pred-trans-job-single-part-1c" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/1c.sql", 1, false);
}

TEST_CASE ("no-pred-trans-job-single-part-1d" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/1d.sql", 1, false);
}

TEST_CASE ("no-pred-trans-job-single-part-2a" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/2a.sql", 1, false);
}

TEST_CASE ("no-pred-trans-job-single-part-2b" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/2b.sql", 1, false);
}

TEST_CASE ("no-pred-trans-job-single-part-2c" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/2c.sql", 1, false);
}

TEST_CASE ("no-pred-trans-job-single-part-2d" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/2d.sql", 1, false);
}

TEST_CASE ("no-pred-trans-job-single-part-3a" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/3a.sql", 1, false);
}

TEST_CASE ("no-pred-trans-job-single-part-3b" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/3b.sql", 1, false);
}

TEST_CASE ("no-pred-trans-job-single-part-3c" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/3c.sql", 1, false);
}

TEST_CASE ("no-pred-trans-job-single-part-4a" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/4a.sql", 1, false);
}

TEST_CASE ("no-pred-trans-job-single-part-4b" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/4b.sql", 1, false);
}

TEST_CASE ("no-pred-trans-job-single-part-4c" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/4c.sql", 1, false);
}

TEST_CASE ("no-pred-trans-job-single-part-5a" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/5a.sql", 1, false);
}

TEST_CASE ("no-pred-trans-job-single-part-5b" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/5b.sql", 1, false);
}

TEST_CASE ("no-pred-trans-job-single-part-5c" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/5c.sql", 1, false);
}

TEST_CASE ("no-pred-trans-job-single-part-6a" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/6a.sql", 1, false);
}

TEST_CASE ("no-pred-trans-job-single-part-6b" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/6b.sql", 1, false);
}

TEST_CASE ("no-pred-trans-job-single-part-6c" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/6c.sql", 1, false);
}

TEST_CASE ("no-pred-trans-job-single-part-6d" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/6d.sql", 1, false);
}

TEST_CASE ("no-pred-trans-job-single-part-6e" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/6e.sql", 1, false);
}

TEST_CASE ("no-pred-trans-job-single-part-6f" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/6f.sql", 1, false);
}

TEST_CASE ("no-pred-trans-job-single-part-7a" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/7a.sql", 1, false);
}

TEST_CASE ("no-pred-trans-job-single-part-7b" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/7b.sql", 1, false);
}

TEST_CASE ("no-pred-trans-job-single-part-7c" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/7c.sql", 1, false);
}

TEST_CASE ("no-pred-trans-job-single-part-8a" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/8a.sql", 1, false);
}

TEST_CASE ("no-pred-trans-job-single-part-8b" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/8b.sql", 1, false);
}

TEST_CASE ("no-pred-trans-job-single-part-8c" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/8c.sql", 1, false);
}

TEST_CASE ("no-pred-trans-job-single-part-8d" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/8d.sql", 1, false);
}

TEST_CASE ("no-pred-trans-job-single-part-9a" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/9a.sql", 1, false);
}

TEST_CASE ("no-pred-trans-job-single-part-9b" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/9b.sql", 1, false);
}

TEST_CASE ("no-pred-trans-job-single-part-9c" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/9c.sql", 1, false);
}

TEST_CASE ("no-pred-trans-job-single-part-9d" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/9d.sql", 1, false);
}

TEST_CASE ("no-pred-trans-job-single-part-10a" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/10a.sql", 1, false);
}

TEST_CASE ("no-pred-trans-job-single-part-10b" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/10b.sql", 1, false);
}

TEST_CASE ("no-pred-trans-job-single-part-10c" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/10c.sql", 1, false);
}

TEST_CASE ("no-pred-trans-job-single-part-11a" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/11a.sql", 1, false);
}

TEST_CASE ("no-pred-trans-job-single-part-11b" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/11b.sql", 1, false);
}

TEST_CASE ("no-pred-trans-job-single-part-11c" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/11c.sql", 1, false);
}

TEST_CASE ("no-pred-trans-job-single-part-11d" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/11d.sql", 1, false);
}

TEST_CASE ("no-pred-trans-job-single-part-12a" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/12a.sql", 1, false);
}

TEST_CASE ("no-pred-trans-job-single-part-12b" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/12b.sql", 1, false);
}

TEST_CASE ("no-pred-trans-job-single-part-12c" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/12c.sql", 1, false);
}

TEST_CASE ("no-pred-trans-job-single-part-13a" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/13a.sql", 1, false);
}

TEST_CASE ("no-pred-trans-job-single-part-13b" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/13b.sql", 1, false);
}

TEST_CASE ("no-pred-trans-job-single-part-13c" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/13c.sql", 1, false);
}

TEST_CASE ("no-pred-trans-job-single-part-13d" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/13d.sql", 1, false);
}

TEST_CASE ("no-pred-trans-job-single-part-14a" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/14a.sql", 1, false);
}

TEST_CASE ("no-pred-trans-job-single-part-14b" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/14b.sql", 1, false);
}

TEST_CASE ("no-pred-trans-job-single-part-14c" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/14c.sql", 1, false);
}

TEST_CASE ("no-pred-trans-job-single-part-15a" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/15a.sql", 1, false);
}

TEST_CASE ("no-pred-trans-job-single-part-15b" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/15b.sql", 1, false);
}

TEST_CASE ("no-pred-trans-job-single-part-15c" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/15c.sql", 1, false);
}

TEST_CASE ("no-pred-trans-job-single-part-15d" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/15d.sql", 1, false);
}

TEST_CASE ("no-pred-trans-job-single-part-16a" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/16a.sql", 1, false);
}

TEST_CASE ("no-pred-trans-job-single-part-16b" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/16b.sql", 1, false);
}

TEST_CASE ("no-pred-trans-job-single-part-16c" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/16c.sql", 1, false);
}

TEST_CASE ("no-pred-trans-job-single-part-16d" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/16d.sql", 1, false);
}

TEST_CASE ("no-pred-trans-job-single-part-17a" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/17a.sql", 1, false);
}

TEST_CASE ("no-pred-trans-job-single-part-17b" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/17b.sql", 1, false);
}

TEST_CASE ("no-pred-trans-job-single-part-17c" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/17c.sql", 1, false);
}

TEST_CASE ("no-pred-trans-job-single-part-17d" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/17d.sql", 1, false);
}

TEST_CASE ("no-pred-trans-job-single-part-17e" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/17e.sql", 1, false);
}

TEST_CASE ("no-pred-trans-job-single-part-17f" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/17f.sql", 1, false);
}

TEST_CASE ("no-pred-trans-job-single-part-18a" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/18a.sql", 1, false);
}

TEST_CASE ("no-pred-trans-job-single-part-18b" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/18b.sql", 1, false);
}

TEST_CASE ("no-pred-trans-job-single-part-18c" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/18c.sql", 1, false);
}

TEST_CASE ("no-pred-trans-job-single-part-19a" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/19a.sql", 1, false);
}

TEST_CASE ("no-pred-trans-job-single-part-19b" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/19b.sql", 1, false);
}

TEST_CASE ("no-pred-trans-job-single-part-19c" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/19c.sql", 1, false);
}

TEST_CASE ("no-pred-trans-job-single-part-19d" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/19d.sql", 1, false);
}

TEST_CASE ("no-pred-trans-job-single-part-20a" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/20a.sql", 1, false);
}

TEST_CASE ("no-pred-trans-job-single-part-20b" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/20b.sql", 1, false);
}

TEST_CASE ("no-pred-trans-job-single-part-20c" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/20c.sql", 1, false);
}

TEST_CASE ("no-pred-trans-job-single-part-21a" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/21a.sql", 1, false);
}

TEST_CASE ("no-pred-trans-job-single-part-21b" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/21b.sql", 1, false);
}

TEST_CASE ("no-pred-trans-job-single-part-21c" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/21c.sql", 1, false);
}

TEST_CASE ("no-pred-trans-job-single-part-22a" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/22a.sql", 1, false);
}

TEST_CASE ("no-pred-trans-job-single-part-22b" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/22b.sql", 1, false);
}

TEST_CASE ("no-pred-trans-job-single-part-22c" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/22c.sql", 1, false);
}

TEST_CASE ("no-pred-trans-job-single-part-22d" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/22d.sql", 1, false);
}

TEST_CASE ("no-pred-trans-job-single-part-23a" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/23a.sql", 1, false);
}

TEST_CASE ("no-pred-trans-job-single-part-23b" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/23b.sql", 1, false);
}

TEST_CASE ("no-pred-trans-job-single-part-23c" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/23c.sql", 1, false);
}

TEST_CASE ("no-pred-trans-job-single-part-24a" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/24a.sql", 1, false);
}

TEST_CASE ("no-pred-trans-job-single-part-24b" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/24b.sql", 1, false);
}

TEST_CASE ("no-pred-trans-job-single-part-25a" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/25a.sql", 1, false);
}

TEST_CASE ("no-pred-trans-job-single-part-25b" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/25b.sql", 1, false);
}

TEST_CASE ("no-pred-trans-job-single-part-25c" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/25c.sql", 1, false);
}

TEST_CASE ("no-pred-trans-job-single-part-26a" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/26a.sql", 1, false);
}

TEST_CASE ("no-pred-trans-job-single-part-26b" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/26b.sql", 1, false);
}

TEST_CASE ("no-pred-trans-job-single-part-26c" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/26c.sql", 1, false);
}

TEST_CASE ("no-pred-trans-job-single-part-27a" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/27a.sql", 1, false);
}

TEST_CASE ("no-pred-trans-job-single-part-27b" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/27b.sql", 1, false);
}

TEST_CASE ("no-pred-trans-job-single-part-27c" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/27c.sql", 1, false);
}

TEST_CASE ("no-pred-trans-job-single-part-28a" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/28a.sql", 1, false);
}

TEST_CASE ("no-pred-trans-job-single-part-28b" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/28b.sql", 1, false);
}

TEST_CASE ("no-pred-trans-job-single-part-28c" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/28c.sql", 1, false);
}

TEST_CASE ("no-pred-trans-job-single-part-29a" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/29a.sql", 1, false);
}

TEST_CASE ("no-pred-trans-job-single-part-29b" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/29b.sql", 1, false);
}

TEST_CASE ("no-pred-trans-job-single-part-29c" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/29c.sql", 1, false);
}

TEST_CASE ("no-pred-trans-job-single-part-30a" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/30a.sql", 1, false);
}

TEST_CASE ("no-pred-trans-job-single-part-30b" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/30b.sql", 1, false);
}

TEST_CASE ("no-pred-trans-job-single-part-30c" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/30c.sql", 1, false);
}

TEST_CASE ("no-pred-trans-job-single-part-31a" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/31a.sql", 1, false);
}

TEST_CASE ("no-pred-trans-job-single-part-31b" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/31b.sql", 1, false);
}

TEST_CASE ("no-pred-trans-job-single-part-31c" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/31c.sql", 1, false);
}

TEST_CASE ("no-pred-trans-job-single-part-32a" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/32a.sql", 1, false);
}

TEST_CASE ("no-pred-trans-job-single-part-32b" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/32b.sql", 1, false);
}

TEST_CASE ("no-pred-trans-job-single-part-33a" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/33a.sql", 1, false);
}

TEST_CASE ("no-pred-trans-job-single-part-33b" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/33b.sql", 1, false);
}

TEST_CASE ("no-pred-trans-job-single-part-33c" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/33c.sql", 1, false);
}

}

// single-partition single thread tests
TEST_SUITE ("pred-trans-job-single-part" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("pred-trans-job-single-part-1a" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/1a.sql", 1);
}

TEST_CASE ("pred-trans-job-single-part-1b" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/1b.sql", 1);
}

TEST_CASE ("pred-trans-job-single-part-1c" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/1c.sql", 1);
}

TEST_CASE ("pred-trans-job-single-part-1d" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/1d.sql", 1);
}

TEST_CASE ("pred-trans-job-single-part-2a" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/2a.sql", 1);
}

TEST_CASE ("pred-trans-job-single-part-2b" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/2b.sql", 1);
}

TEST_CASE ("pred-trans-job-single-part-2c" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/2c.sql", 1);
}

TEST_CASE ("pred-trans-job-single-part-2d" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/2d.sql", 1);
}

TEST_CASE ("pred-trans-job-single-part-3a" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/3a.sql", 1);
}

TEST_CASE ("pred-trans-job-single-part-3b" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/3b.sql", 1);
}

TEST_CASE ("pred-trans-job-single-part-3c" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/3c.sql", 1);
}

TEST_CASE ("pred-trans-job-single-part-4a" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/4a.sql", 1);
}

TEST_CASE ("pred-trans-job-single-part-4b" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/4b.sql", 1);
}

TEST_CASE ("pred-trans-job-single-part-4c" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/4c.sql", 1);
}

TEST_CASE ("pred-trans-job-single-part-5a" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/5a.sql", 1);
}

TEST_CASE ("pred-trans-job-single-part-5b" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/5b.sql", 1);
}

TEST_CASE ("pred-trans-job-single-part-5c" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/5c.sql", 1);
}

TEST_CASE ("pred-trans-job-single-part-6a" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/6a.sql", 1);
}

TEST_CASE ("pred-trans-job-single-part-6b" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/6b.sql", 1);
}

TEST_CASE ("pred-trans-job-single-part-6c" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/6c.sql", 1);
}

TEST_CASE ("pred-trans-job-single-part-6d" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/6d.sql", 1);
}

TEST_CASE ("pred-trans-job-single-part-6e" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/6e.sql", 1);
}

TEST_CASE ("pred-trans-job-single-part-6f" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/6f.sql", 1);
}

TEST_CASE ("pred-trans-job-single-part-7a" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/7a.sql", 1);
}

TEST_CASE ("pred-trans-job-single-part-7b" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/7b.sql", 1);
}

TEST_CASE ("pred-trans-job-single-part-7c" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/7c.sql", 1);
}

TEST_CASE ("pred-trans-job-single-part-8a" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/8a.sql", 1);
}

TEST_CASE ("pred-trans-job-single-part-8b" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/8b.sql", 1);
}

TEST_CASE ("pred-trans-job-single-part-8c" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/8c.sql", 1);
}

TEST_CASE ("pred-trans-job-single-part-8d" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/8d.sql", 1);
}

TEST_CASE ("pred-trans-job-single-part-9a" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/9a.sql", 1);
}

TEST_CASE ("pred-trans-job-single-part-9b" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/9b.sql", 1);
}

TEST_CASE ("pred-trans-job-single-part-9c" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/9c.sql", 1);
}

TEST_CASE ("pred-trans-job-single-part-9d" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/9d.sql", 1);
}

TEST_CASE ("pred-trans-job-single-part-10a" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/10a.sql", 1);
}

TEST_CASE ("pred-trans-job-single-part-10b" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/10b.sql", 1);
}

TEST_CASE ("pred-trans-job-single-part-10c" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/10c.sql", 1);
}

TEST_CASE ("pred-trans-job-single-part-11a" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/11a.sql", 1);
}

TEST_CASE ("pred-trans-job-single-part-11b" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/11b.sql", 1);
}

TEST_CASE ("pred-trans-job-single-part-11c" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/11c.sql", 1);
}

TEST_CASE ("pred-trans-job-single-part-11d" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/11d.sql", 1);
}

TEST_CASE ("pred-trans-job-single-part-12a" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/12a.sql", 1);
}

TEST_CASE ("pred-trans-job-single-part-12b" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/12b.sql", 1);
}

TEST_CASE ("pred-trans-job-single-part-12c" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/12c.sql", 1);
}

TEST_CASE ("pred-trans-job-single-part-13a" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/13a.sql", 1);
}

TEST_CASE ("pred-trans-job-single-part-13b" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/13b.sql", 1);
}

TEST_CASE ("pred-trans-job-single-part-13c" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/13c.sql", 1);
}

TEST_CASE ("pred-trans-job-single-part-13d" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/13d.sql", 1);
}

TEST_CASE ("pred-trans-job-single-part-14a" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/14a.sql", 1);
}

TEST_CASE ("pred-trans-job-single-part-14b" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/14b.sql", 1);
}

TEST_CASE ("pred-trans-job-single-part-14c" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/14c.sql", 1);
}

TEST_CASE ("pred-trans-job-single-part-15a" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/15a.sql", 1);
}

TEST_CASE ("pred-trans-job-single-part-15b" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/15b.sql", 1);
}

TEST_CASE ("pred-trans-job-single-part-15c" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/15c.sql", 1);
}

TEST_CASE ("pred-trans-job-single-part-15d" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/15d.sql", 1);
}

TEST_CASE ("pred-trans-job-single-part-16a" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/16a.sql", 1);
}

TEST_CASE ("pred-trans-job-single-part-16b" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/16b.sql", 1);
}

TEST_CASE ("pred-trans-job-single-part-16c" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/16c.sql", 1);
}

TEST_CASE ("pred-trans-job-single-part-16d" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/16d.sql", 1);
}

TEST_CASE ("pred-trans-job-single-part-17a" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/17a.sql", 1);
}

TEST_CASE ("pred-trans-job-single-part-17b" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/17b.sql", 1);
}

TEST_CASE ("pred-trans-job-single-part-17c" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/17c.sql", 1);
}

TEST_CASE ("pred-trans-job-single-part-17d" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/17d.sql", 1);
}

TEST_CASE ("pred-trans-job-single-part-17e" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/17e.sql", 1);
}

TEST_CASE ("pred-trans-job-single-part-17f" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/17f.sql", 1);
}

TEST_CASE ("pred-trans-job-single-part-18a" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/18a.sql", 1);
}

TEST_CASE ("pred-trans-job-single-part-18b" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/18b.sql", 1);
}

TEST_CASE ("pred-trans-job-single-part-18c" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/18c.sql", 1);
}

TEST_CASE ("pred-trans-job-single-part-19a" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/19a.sql", 1);
}

TEST_CASE ("pred-trans-job-single-part-19b" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/19b.sql", 1);
}

TEST_CASE ("pred-trans-job-single-part-19c" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/19c.sql", 1);
}

TEST_CASE ("pred-trans-job-single-part-19d" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/19d.sql", 1);
}

TEST_CASE ("pred-trans-job-single-part-20a" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/20a.sql", 1);
}

TEST_CASE ("pred-trans-job-single-part-20b" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/20b.sql", 1);
}

TEST_CASE ("pred-trans-job-single-part-20c" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/20c.sql", 1);
}

TEST_CASE ("pred-trans-job-single-part-21a" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/21a.sql", 1);
}

TEST_CASE ("pred-trans-job-single-part-21b" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/21b.sql", 1);
}

TEST_CASE ("pred-trans-job-single-part-21c" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/21c.sql", 1);
}

TEST_CASE ("pred-trans-job-single-part-22a" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/22a.sql", 1);
}

TEST_CASE ("pred-trans-job-single-part-22b" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/22b.sql", 1);
}

TEST_CASE ("pred-trans-job-single-part-22c" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/22c.sql", 1);
}

TEST_CASE ("pred-trans-job-single-part-22d" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/22d.sql", 1);
}

TEST_CASE ("pred-trans-job-single-part-23a" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/23a.sql", 1);
}

TEST_CASE ("pred-trans-job-single-part-23b" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/23b.sql", 1);
}

TEST_CASE ("pred-trans-job-single-part-23c" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/23c.sql", 1);
}

TEST_CASE ("pred-trans-job-single-part-24a" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/24a.sql", 1);
}

TEST_CASE ("pred-trans-job-single-part-24b" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/24b.sql", 1);
}

TEST_CASE ("pred-trans-job-single-part-25a" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/25a.sql", 1);
}

TEST_CASE ("pred-trans-job-single-part-25b" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/25b.sql", 1);
}

TEST_CASE ("pred-trans-job-single-part-25c" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/25c.sql", 1);
}

TEST_CASE ("pred-trans-job-single-part-26a" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/26a.sql", 1);
}

TEST_CASE ("pred-trans-job-single-part-26b" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/26b.sql", 1);
}

TEST_CASE ("pred-trans-job-single-part-26c" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/26c.sql", 1);
}

TEST_CASE ("pred-trans-job-single-part-27a" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/27a.sql", 1);
}

TEST_CASE ("pred-trans-job-single-part-27b" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/27b.sql", 1);
}

TEST_CASE ("pred-trans-job-single-part-27c" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/27c.sql", 1);
}

TEST_CASE ("pred-trans-job-single-part-28a" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/28a.sql", 1);
}

TEST_CASE ("pred-trans-job-single-part-28b" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/28b.sql", 1);
}

TEST_CASE ("pred-trans-job-single-part-28c" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/28c.sql", 1);
}

TEST_CASE ("pred-trans-job-single-part-29a" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/29a.sql", 1);
}

TEST_CASE ("pred-trans-job-single-part-29b" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/29b.sql", 1);
}

TEST_CASE ("pred-trans-job-single-part-29c" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/29c.sql", 1);
}

TEST_CASE ("pred-trans-job-single-part-30a" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/30a.sql", 1);
}

TEST_CASE ("pred-trans-job-single-part-30b" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/30b.sql", 1);
}

TEST_CASE ("pred-trans-job-single-part-30c" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/30c.sql", 1);
}

TEST_CASE ("pred-trans-job-single-part-31a" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/31a.sql", 1);
}

TEST_CASE ("pred-trans-job-single-part-31b" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/31b.sql", 1);
}

TEST_CASE ("pred-trans-job-single-part-31c" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/31c.sql", 1);
}

TEST_CASE ("pred-trans-job-single-part-32a" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/32a.sql", 1);
}

TEST_CASE ("pred-trans-job-single-part-32b" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/32b.sql", 1);
}

TEST_CASE ("pred-trans-job-single-part-33a" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/33a.sql", 1);
}

TEST_CASE ("pred-trans-job-single-part-33b" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/33b.sql", 1);
}

TEST_CASE ("pred-trans-job-single-part-33c" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/33c.sql", 1);
}

}

// single-partition single thread tests
TEST_SUITE ("yannakakis-job-single-part" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("yannakakis-job-single-part-1a" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/1a.sql", 1, true, true);
}

TEST_CASE ("yannakakis-job-single-part-1b" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/1b.sql", 1, true, true);
}

TEST_CASE ("yannakakis-job-single-part-1c" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/1c.sql", 1, true, true);
}

TEST_CASE ("yannakakis-job-single-part-1d" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/1d.sql", 1, true, true);
}

TEST_CASE ("yannakakis-job-single-part-2a" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/2a.sql", 1, true, true);
}

TEST_CASE ("yannakakis-job-single-part-2b" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/2b.sql", 1, true, true);
}

TEST_CASE ("yannakakis-job-single-part-2c" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/2c.sql", 1, true, true);
}

TEST_CASE ("yannakakis-job-single-part-2d" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/2d.sql", 1, true, true);
}

TEST_CASE ("yannakakis-job-single-part-3a" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/3a.sql", 1, true, true);
}

TEST_CASE ("yannakakis-job-single-part-3b" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/3b.sql", 1, true, true);
}

TEST_CASE ("yannakakis-job-single-part-3c" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/3c.sql", 1, true, true);
}

TEST_CASE ("yannakakis-job-single-part-4a" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/4a.sql", 1, true, true);
}

TEST_CASE ("yannakakis-job-single-part-4b" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/4b.sql", 1, true, true);
}

TEST_CASE ("yannakakis-job-single-part-4c" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/4c.sql", 1, true, true);
}

TEST_CASE ("yannakakis-job-single-part-5a" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/5a.sql", 1, true, true);
}

TEST_CASE ("yannakakis-job-single-part-5b" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/5b.sql", 1, true, true);
}

TEST_CASE ("yannakakis-job-single-part-5c" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/5c.sql", 1, true, true);
}

TEST_CASE ("yannakakis-job-single-part-6a" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/6a.sql", 1, true, true);
}

TEST_CASE ("yannakakis-job-single-part-6b" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/6b.sql", 1, true, true);
}

TEST_CASE ("yannakakis-job-single-part-6c" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/6c.sql", 1, true, true);
}

TEST_CASE ("yannakakis-job-single-part-6d" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/6d.sql", 1, true, true);
}

TEST_CASE ("yannakakis-job-single-part-6e" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/6e.sql", 1, true, true);
}

TEST_CASE ("yannakakis-job-single-part-6f" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/6f.sql", 1, true, true);
}

TEST_CASE ("yannakakis-job-single-part-7a" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/7a.sql", 1, true, true);
}

TEST_CASE ("yannakakis-job-single-part-7b" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/7b.sql", 1, true, true);
}

TEST_CASE ("yannakakis-job-single-part-7c" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/7c.sql", 1, true, true);
}

TEST_CASE ("yannakakis-job-single-part-8a" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/8a.sql", 1, true, true);
}

TEST_CASE ("yannakakis-job-single-part-8b" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/8b.sql", 1, true, true);
}

TEST_CASE ("yannakakis-job-single-part-8c" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/8c.sql", 1, true, true);
}

TEST_CASE ("yannakakis-job-single-part-8d" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/8d.sql", 1, true, true);
}

TEST_CASE ("yannakakis-job-single-part-9a" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/9a.sql", 1, true, true);
}

TEST_CASE ("yannakakis-job-single-part-9b" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/9b.sql", 1, true, true);
}

TEST_CASE ("yannakakis-job-single-part-9c" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/9c.sql", 1, true, true);
}

TEST_CASE ("yannakakis-job-single-part-9d" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/9d.sql", 1, true, true);
}

TEST_CASE ("yannakakis-job-single-part-10a" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/10a.sql", 1, true, true);
}

TEST_CASE ("yannakakis-job-single-part-10b" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/10b.sql", 1, true, true);
}

TEST_CASE ("yannakakis-job-single-part-10c" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/10c.sql", 1, true, true);
}

TEST_CASE ("yannakakis-job-single-part-11a" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/11a.sql", 1, true, true);
}

TEST_CASE ("yannakakis-job-single-part-11b" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/11b.sql", 1, true, true);
}

TEST_CASE ("yannakakis-job-single-part-11c" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/11c.sql", 1, true, true);
}

TEST_CASE ("yannakakis-job-single-part-11d" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/11d.sql", 1, true, true);
}

TEST_CASE ("yannakakis-job-single-part-12a" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/12a.sql", 1, true, true);
}

TEST_CASE ("yannakakis-job-single-part-12b" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/12b.sql", 1, true, true);
}

TEST_CASE ("yannakakis-job-single-part-12c" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/12c.sql", 1, true, true);
}

TEST_CASE ("yannakakis-job-single-part-13a" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/13a.sql", 1, true, true);
}

TEST_CASE ("yannakakis-job-single-part-13b" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/13b.sql", 1, true, true);
}

TEST_CASE ("yannakakis-job-single-part-13c" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/13c.sql", 1, true, true);
}

TEST_CASE ("yannakakis-job-single-part-13d" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/13d.sql", 1, true, true);
}

TEST_CASE ("yannakakis-job-single-part-14a" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/14a.sql", 1, true, true);
}

TEST_CASE ("yannakakis-job-single-part-14b" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/14b.sql", 1, true, true);
}

TEST_CASE ("yannakakis-job-single-part-14c" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/14c.sql", 1, true, true);
}

TEST_CASE ("yannakakis-job-single-part-15a" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/15a.sql", 1, true, true);
}

TEST_CASE ("yannakakis-job-single-part-15b" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/15b.sql", 1, true, true);
}

TEST_CASE ("yannakakis-job-single-part-15c" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/15c.sql", 1, true, true);
}

TEST_CASE ("yannakakis-job-single-part-15d" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/15d.sql", 1, true, true);
}

TEST_CASE ("yannakakis-job-single-part-16a" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/16a.sql", 1, true, true);
}

TEST_CASE ("yannakakis-job-single-part-16b" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/16b.sql", 1, true, true);
}

TEST_CASE ("yannakakis-job-single-part-16c" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/16c.sql", 1, true, true);
}

TEST_CASE ("yannakakis-job-single-part-16d" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/16d.sql", 1, true, true);
}

TEST_CASE ("yannakakis-job-single-part-17a" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/17a.sql", 1, true, true);
}

TEST_CASE ("yannakakis-job-single-part-17b" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/17b.sql", 1, true, true);
}

TEST_CASE ("yannakakis-job-single-part-17c" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/17c.sql", 1, true, true);
}

TEST_CASE ("yannakakis-job-single-part-17d" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/17d.sql", 1, true, true);
}

TEST_CASE ("yannakakis-job-single-part-17e" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/17e.sql", 1, true, true);
}

TEST_CASE ("yannakakis-job-single-part-17f" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/17f.sql", 1, true, true);
}

TEST_CASE ("yannakakis-job-single-part-18a" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/18a.sql", 1, true, true);
}

TEST_CASE ("yannakakis-job-single-part-18b" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/18b.sql", 1, true, true);
}

TEST_CASE ("yannakakis-job-single-part-18c" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/18c.sql", 1, true, true);
}

TEST_CASE ("yannakakis-job-single-part-19a" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/19a.sql", 1, true, true);
}

TEST_CASE ("yannakakis-job-single-part-19b" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/19b.sql", 1, true, true);
}

TEST_CASE ("yannakakis-job-single-part-19c" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/19c.sql", 1, true, true);
}

TEST_CASE ("yannakakis-job-single-part-19d" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/19d.sql", 1, true, true);
}

TEST_CASE ("yannakakis-job-single-part-20a" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/20a.sql", 1, true, true);
}

TEST_CASE ("yannakakis-job-single-part-20b" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/20b.sql", 1, true, true);
}

TEST_CASE ("yannakakis-job-single-part-20c" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/20c.sql", 1, true, true);
}

TEST_CASE ("yannakakis-job-single-part-21a" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/21a.sql", 1, true, true);
}

TEST_CASE ("yannakakis-job-single-part-21b" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/21b.sql", 1, true, true);
}

TEST_CASE ("yannakakis-job-single-part-21c" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/21c.sql", 1, true, true);
}

TEST_CASE ("yannakakis-job-single-part-22a" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/22a.sql", 1, true, true);
}

TEST_CASE ("yannakakis-job-single-part-22b" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/22b.sql", 1, true, true);
}

TEST_CASE ("yannakakis-job-single-part-22c" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/22c.sql", 1, true, true);
}

TEST_CASE ("yannakakis-job-single-part-22d" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/22d.sql", 1, true, true);
}

TEST_CASE ("yannakakis-job-single-part-23a" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/23a.sql", 1, true, true);
}

TEST_CASE ("yannakakis-job-single-part-23b" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/23b.sql", 1, true, true);
}

TEST_CASE ("yannakakis-job-single-part-23c" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/23c.sql", 1, true, true);
}

TEST_CASE ("yannakakis-job-single-part-24a" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/24a.sql", 1, true, true);
}

TEST_CASE ("yannakakis-job-single-part-24b" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/24b.sql", 1, true, true);
}

TEST_CASE ("yannakakis-job-single-part-25a" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/25a.sql", 1, true, true);
}

TEST_CASE ("yannakakis-job-single-part-25b" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/25b.sql", 1, true, true);
}

TEST_CASE ("yannakakis-job-single-part-25c" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/25c.sql", 1, true, true);
}

TEST_CASE ("yannakakis-job-single-part-26a" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/26a.sql", 1, true, true);
}

TEST_CASE ("yannakakis-job-single-part-26b" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/26b.sql", 1, true, true);
}

TEST_CASE ("yannakakis-job-single-part-26c" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/26c.sql", 1, true, true);
}

TEST_CASE ("yannakakis-job-single-part-27a" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/27a.sql", 1, true, true);
}

TEST_CASE ("yannakakis-job-single-part-27b" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/27b.sql", 1, true, true);
}

TEST_CASE ("yannakakis-job-single-part-27c" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/27c.sql", 1, true, true);
}

TEST_CASE ("yannakakis-job-single-part-28a" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/28a.sql", 1, true, true);
}

TEST_CASE ("yannakakis-job-single-part-28b" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/28b.sql", 1, true, true);
}

TEST_CASE ("yannakakis-job-single-part-28c" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/28c.sql", 1, true, true);
}

TEST_CASE ("yannakakis-job-single-part-29a" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/29a.sql", 1, true, true);
}

TEST_CASE ("yannakakis-job-single-part-29b" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/29b.sql", 1, true, true);
}

TEST_CASE ("yannakakis-job-single-part-29c" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/29c.sql", 1, true, true);
}

TEST_CASE ("yannakakis-job-single-part-30a" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/30a.sql", 1, true, true);
}

TEST_CASE ("yannakakis-job-single-part-30b" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/30b.sql", 1, true, true);
}

TEST_CASE ("yannakakis-job-single-part-30c" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/30c.sql", 1, true, true);
}

TEST_CASE ("yannakakis-job-single-part-31a" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/31a.sql", 1, true, true);
}

TEST_CASE ("yannakakis-job-single-part-31b" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/31b.sql", 1, true, true);
}

TEST_CASE ("yannakakis-job-single-part-31c" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/31c.sql", 1, true, true);
}

TEST_CASE ("yannakakis-job-single-part-32a" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/32a.sql", 1, true, true);
}

TEST_CASE ("yannakakis-job-single-part-32b" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/32b.sql", 1, true, true);
}

TEST_CASE ("yannakakis-job-single-part-33a" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/33a.sql", 1, true, true);
}

TEST_CASE ("yannakakis-job-single-part-33b" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/33b.sql", 1, true, true);
}

TEST_CASE ("yannakakis-job-single-part-33c" * doctest::skip(false || SKIP_SUITE)) {
  PredTransTestUtil::testPredTrans("job/parquet/", "job/original/33c.sql", 1, true, true);
}

}

}
