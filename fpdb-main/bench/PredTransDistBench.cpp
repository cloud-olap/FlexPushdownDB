//
// Created by Yifei Yang on 12/18/23.
//

#include "PredTransTestUtil.h"
#include "Globals.h"
#include <doctest/doctest.h>
#include <fpdb/executor/physical/Globals.h>

/**
 * Predicate transfer distributed bench
 *
 * Multiple compute nodes, read data from S3
 * Start Calcite server on the coordinator (locally) and CAF server on all executors (remote nodes) before running this
 * (see README for more details)
 */
namespace fpdb::main::test {

#define SKIP_SUITE false

TEST_SUITE ("pred-trans-tpch-custom-dist" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("pred-trans-tpch-custom-dist-01" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_TPCH_CUSTOM.data());
  PredTransTestUtil::testPredTransDist(std::string(SCHEMA_TPCH_CUSTOM), "tpch/original/01.sql",
                                       PARALLEL_DIST_CUSTOM);
}

TEST_CASE ("pred-trans-tpch-custom-dist-02" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_TPCH_CUSTOM.data());
  PredTransTestUtil::testPredTransDist(std::string(SCHEMA_TPCH_CUSTOM), "tpch/original/02.sql",
                                       PARALLEL_DIST_CUSTOM);
}

TEST_CASE ("pred-trans-tpch-custom-dist-03" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_TPCH_CUSTOM.data());
  PredTransTestUtil::testPredTransDist(std::string(SCHEMA_TPCH_CUSTOM), "tpch/original/03.sql",
                                       PARALLEL_DIST_CUSTOM);
}

TEST_CASE ("pred-trans-tpch-custom-dist-04" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_TPCH_CUSTOM.data());
  PredTransTestUtil::testPredTransDist(std::string(SCHEMA_TPCH_CUSTOM), "tpch/original/04.sql",
                                       PARALLEL_DIST_CUSTOM);
}

// force to use join order 2
TEST_CASE ("pred-trans-tpch-custom-dist-05" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_TPCH_CUSTOM.data());
  PredTransTestUtil::testPredTransDist(std::string(SCHEMA_TPCH_CUSTOM), "tpch/modified/05-jo2.sql",
                                       PARALLEL_DIST_CUSTOM, true, false);
}

TEST_CASE ("pred-trans-tpch-custom-dist-06" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_TPCH_CUSTOM.data());
  PredTransTestUtil::testPredTransDist(std::string(SCHEMA_TPCH_CUSTOM), "tpch/original/06.sql",
                                       PARALLEL_DIST_CUSTOM);
}

TEST_CASE ("pred-trans-tpch-custom-dist-07" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_TPCH_CUSTOM.data());
  PredTransTestUtil::testPredTransDist(std::string(SCHEMA_TPCH_CUSTOM), "tpch/original/07.sql",
                                       PARALLEL_DIST_CUSTOM);
}

TEST_CASE ("pred-trans-tpch-custom-dist-08" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_TPCH_CUSTOM.data());
  PredTransTestUtil::testPredTransDist(std::string(SCHEMA_TPCH_CUSTOM), "tpch/original/08.sql",
                                       PARALLEL_DIST_CUSTOM);
}

TEST_CASE ("pred-trans-tpch-custom-dist-09" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_TPCH_CUSTOM.data());
  PredTransTestUtil::testPredTransDist(std::string(SCHEMA_TPCH_CUSTOM), "tpch/original/09.sql",
                                       PARALLEL_DIST_CUSTOM);
}

TEST_CASE ("pred-trans-tpch-custom-dist-10" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_TPCH_CUSTOM.data());
  PredTransTestUtil::testPredTransDist(std::string(SCHEMA_TPCH_CUSTOM), "tpch/original/10.sql",
                                       PARALLEL_DIST_CUSTOM);
}

TEST_CASE ("pred-trans-tpch-custom-dist-11" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_TPCH_CUSTOM.data());
  PredTransTestUtil::testPredTransDist(std::string(SCHEMA_TPCH_CUSTOM), "tpch/original/11.sql",
                                       PARALLEL_DIST_CUSTOM);
}

TEST_CASE ("pred-trans-tpch-custom-dist-12" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_TPCH_CUSTOM.data());
  PredTransTestUtil::testPredTransDist(std::string(SCHEMA_TPCH_CUSTOM), "tpch/original/12.sql",
                                       PARALLEL_DIST_CUSTOM);
}

TEST_CASE ("pred-trans-tpch-custom-dist-13" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_TPCH_CUSTOM.data());
  PredTransTestUtil::testPredTransDist(std::string(SCHEMA_TPCH_CUSTOM), "tpch/original/13.sql",
                                       PARALLEL_DIST_CUSTOM);
}

TEST_CASE ("pred-trans-tpch-custom-dist-14" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_TPCH_CUSTOM.data());
  PredTransTestUtil::testPredTransDist(std::string(SCHEMA_TPCH_CUSTOM), "tpch/original/14.sql",
                                       PARALLEL_DIST_CUSTOM);
}

TEST_CASE ("pred-trans-tpch-custom-dist-15" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_TPCH_CUSTOM.data());
  PredTransTestUtil::testPredTransDist(std::string(SCHEMA_TPCH_CUSTOM), "tpch/original/15.sql",
                                       PARALLEL_DIST_CUSTOM);
}

TEST_CASE ("pred-trans-tpch-custom-dist-16" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_TPCH_CUSTOM.data());
  PredTransTestUtil::testPredTransDist(std::string(SCHEMA_TPCH_CUSTOM), "tpch/original/16.sql",
                                       PARALLEL_DIST_CUSTOM);
}

TEST_CASE ("pred-trans-tpch-custom-dist-17" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_TPCH_CUSTOM.data());
  PredTransTestUtil::testPredTransDist(std::string(SCHEMA_TPCH_CUSTOM), "tpch/original/17.sql",
                                       PARALLEL_DIST_CUSTOM);
}

TEST_CASE ("pred-trans-tpch-custom-dist-18" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_TPCH_CUSTOM.data());
  PredTransTestUtil::testPredTransDist(std::string(SCHEMA_TPCH_CUSTOM), "tpch/original/18.sql",
                                       PARALLEL_DIST_CUSTOM);
}

TEST_CASE ("pred-trans-tpch-custom-dist-19" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_TPCH_CUSTOM.data());
  PredTransTestUtil::testPredTransDist(std::string(SCHEMA_TPCH_CUSTOM), "tpch/original/19.sql",
                                       PARALLEL_DIST_CUSTOM);
}

TEST_CASE ("pred-trans-tpch-custom-dist-20" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_TPCH_CUSTOM.data());
  PredTransTestUtil::testPredTransDist(std::string(SCHEMA_TPCH_CUSTOM), "tpch/original/20.sql",
                                       PARALLEL_DIST_CUSTOM);
}

TEST_CASE ("pred-trans-tpch-custom-dist-21" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_TPCH_CUSTOM.data());
  PredTransTestUtil::testPredTransDist(std::string(SCHEMA_TPCH_CUSTOM), "tpch/original/21.sql",
                                       PARALLEL_DIST_CUSTOM);
}

TEST_CASE ("pred-trans-tpch-custom-dist-22" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_TPCH_CUSTOM.data());
  PredTransTestUtil::testPredTransDist(std::string(SCHEMA_TPCH_CUSTOM), "tpch/original/22.sql",
                                       PARALLEL_DIST_CUSTOM);
}

}

TEST_SUITE ("no-pred-trans-tpch-custom-dist" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("no-pred-trans-tpch-custom-dist-01" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_TPCH_CUSTOM.data());
  PredTransTestUtil::testPredTransDist(std::string(SCHEMA_TPCH_CUSTOM), "tpch/original/01.sql",
                                       PARALLEL_DIST_CUSTOM, false);
}

TEST_CASE ("no-pred-trans-tpch-custom-dist-02" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_TPCH_CUSTOM.data());
  PredTransTestUtil::testPredTransDist(std::string(SCHEMA_TPCH_CUSTOM), "tpch/original/02.sql",
                                       PARALLEL_DIST_CUSTOM, false);
}

TEST_CASE ("no-pred-trans-tpch-custom-dist-03" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_TPCH_CUSTOM.data());
  PredTransTestUtil::testPredTransDist(std::string(SCHEMA_TPCH_CUSTOM), "tpch/original/03.sql",
                                       PARALLEL_DIST_CUSTOM, false);
}

TEST_CASE ("no-pred-trans-tpch-custom-dist-04" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_TPCH_CUSTOM.data());
  PredTransTestUtil::testPredTransDist(std::string(SCHEMA_TPCH_CUSTOM), "tpch/original/04.sql",
                                       PARALLEL_DIST_CUSTOM, false);
}

// force to use join order 2
TEST_CASE ("no-pred-trans-tpch-custom-dist-05" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_TPCH_CUSTOM.data());
  PredTransTestUtil::testPredTransDist(std::string(SCHEMA_TPCH_CUSTOM), "tpch/modified/05-jo2.sql",
                                       PARALLEL_DIST_CUSTOM, false, false);
}

TEST_CASE ("no-pred-trans-tpch-custom-dist-06" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_TPCH_CUSTOM.data());
  PredTransTestUtil::testPredTransDist(std::string(SCHEMA_TPCH_CUSTOM), "tpch/original/06.sql",
                                       PARALLEL_DIST_CUSTOM, false);
}

TEST_CASE ("no-pred-trans-tpch-custom-dist-07" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_TPCH_CUSTOM.data());
  PredTransTestUtil::testPredTransDist(std::string(SCHEMA_TPCH_CUSTOM), "tpch/original/07.sql",
                                       PARALLEL_DIST_CUSTOM, false);
}

TEST_CASE ("no-pred-trans-tpch-custom-dist-08" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_TPCH_CUSTOM.data());
  PredTransTestUtil::testPredTransDist(std::string(SCHEMA_TPCH_CUSTOM), "tpch/original/08.sql",
                                       PARALLEL_DIST_CUSTOM, false);
}

TEST_CASE ("no-pred-trans-tpch-custom-dist-09" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_TPCH_CUSTOM.data());
  PredTransTestUtil::testPredTransDist(std::string(SCHEMA_TPCH_CUSTOM), "tpch/original/09.sql",
                                       PARALLEL_DIST_CUSTOM, false);
}

TEST_CASE ("no-pred-trans-tpch-custom-dist-10" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_TPCH_CUSTOM.data());
  PredTransTestUtil::testPredTransDist(std::string(SCHEMA_TPCH_CUSTOM), "tpch/original/10.sql",
                                       PARALLEL_DIST_CUSTOM, false);
}

TEST_CASE ("no-pred-trans-tpch-custom-dist-11" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_TPCH_CUSTOM.data());
  PredTransTestUtil::testPredTransDist(std::string(SCHEMA_TPCH_CUSTOM), "tpch/original/11.sql",
                                       PARALLEL_DIST_CUSTOM, false);
}

TEST_CASE ("no-pred-trans-tpch-custom-dist-12" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_TPCH_CUSTOM.data());
  PredTransTestUtil::testPredTransDist(std::string(SCHEMA_TPCH_CUSTOM), "tpch/original/12.sql",
                                       PARALLEL_DIST_CUSTOM, false);
}

TEST_CASE ("no-pred-trans-tpch-custom-dist-13" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_TPCH_CUSTOM.data());
  PredTransTestUtil::testPredTransDist(std::string(SCHEMA_TPCH_CUSTOM), "tpch/original/13.sql",
                                       PARALLEL_DIST_CUSTOM, false);
}

TEST_CASE ("no-pred-trans-tpch-custom-dist-14" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_TPCH_CUSTOM.data());
  PredTransTestUtil::testPredTransDist(std::string(SCHEMA_TPCH_CUSTOM), "tpch/original/14.sql",
                                       PARALLEL_DIST_CUSTOM, false);
}

TEST_CASE ("no-pred-trans-tpch-custom-dist-15" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_TPCH_CUSTOM.data());
  PredTransTestUtil::testPredTransDist(std::string(SCHEMA_TPCH_CUSTOM), "tpch/original/15.sql",
                                       PARALLEL_DIST_CUSTOM, false);
}

TEST_CASE ("no-pred-trans-tpch-custom-dist-16" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_TPCH_CUSTOM.data());
  PredTransTestUtil::testPredTransDist(std::string(SCHEMA_TPCH_CUSTOM), "tpch/original/16.sql",
                                       PARALLEL_DIST_CUSTOM, false);
}

TEST_CASE ("no-pred-trans-tpch-custom-dist-17" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_TPCH_CUSTOM.data());
  PredTransTestUtil::testPredTransDist(std::string(SCHEMA_TPCH_CUSTOM), "tpch/original/17.sql",
                                       PARALLEL_DIST_CUSTOM, false);
}

TEST_CASE ("no-pred-trans-tpch-custom-dist-18" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_TPCH_CUSTOM.data());
  PredTransTestUtil::testPredTransDist(std::string(SCHEMA_TPCH_CUSTOM), "tpch/original/18.sql",
                                       PARALLEL_DIST_CUSTOM, false);
}

TEST_CASE ("no-pred-trans-tpch-custom-dist-19" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_TPCH_CUSTOM.data());
  PredTransTestUtil::testPredTransDist(std::string(SCHEMA_TPCH_CUSTOM), "tpch/original/19.sql",
                                       PARALLEL_DIST_CUSTOM, false);
}

TEST_CASE ("no-pred-trans-tpch-custom-dist-20" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_TPCH_CUSTOM.data());
  PredTransTestUtil::testPredTransDist(std::string(SCHEMA_TPCH_CUSTOM), "tpch/original/20.sql",
                                       PARALLEL_DIST_CUSTOM, false);
}

TEST_CASE ("no-pred-trans-tpch-custom-dist-21" * doctest::skip(false || SKIP_SUITE)) {
  if (executor::physical::DIST_JOIN_TYPE != executor::physical::join::DistJoinType::PTION) {
    executor::physical::TEMP_FIX_TPCH_Q21 = true;
  }
  printf("[CUSTOM] on %s\n", SCHEMA_TPCH_CUSTOM.data());
  PredTransTestUtil::testPredTransDist(std::string(SCHEMA_TPCH_CUSTOM), "tpch/original/21.sql",
                                       PARALLEL_DIST_CUSTOM, false);
  executor::physical::TEMP_FIX_TPCH_Q21 = false;
}

TEST_CASE ("no-pred-trans-tpch-custom-dist-22" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_TPCH_CUSTOM.data());
  PredTransTestUtil::testPredTransDist(std::string(SCHEMA_TPCH_CUSTOM), "tpch/original/22.sql",
                                       PARALLEL_DIST_CUSTOM, false);
}

}

TEST_SUITE ("pred-trans-dsb-custom-dist-spj" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("pred-trans-dsb-custom-dist-spj-13" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_DSB_CUSTOM.data());
  PredTransTestUtil::testPredTransDist(std::string(SCHEMA_DSB_CUSTOM), "dsb/original/spj/q13_spj.sql",
                                       PARALLEL_DIST_CUSTOM);
}

TEST_CASE ("pred-trans-dsb-custom-dist-spj-18" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_DSB_CUSTOM.data());
  PredTransTestUtil::testPredTransDist(std::string(SCHEMA_DSB_CUSTOM), "dsb/original/spj/q18_spj.sql",
                                       PARALLEL_DIST_CUSTOM);
}

TEST_CASE ("pred-trans-dsb-custom-dist-spj-19" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_DSB_CUSTOM.data());
  PredTransTestUtil::testPredTransDist(std::string(SCHEMA_DSB_CUSTOM), "dsb/original/spj/q19_spj.sql",
                                       PARALLEL_DIST_CUSTOM);
}

TEST_CASE ("pred-trans-dsb-custom-dist-spj-25" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_DSB_CUSTOM.data());
  PredTransTestUtil::testPredTransDist(std::string(SCHEMA_DSB_CUSTOM), "dsb/original/spj/q25_spj.sql",
                                       PARALLEL_DIST_CUSTOM);
}

TEST_CASE ("pred-trans-dsb-custom-dist-spj-27" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_DSB_CUSTOM.data());
  PredTransTestUtil::testPredTransDist(std::string(SCHEMA_DSB_CUSTOM), "dsb/original/spj/q27_spj.sql",
                                       PARALLEL_DIST_CUSTOM);
}

TEST_CASE ("pred-trans-dsb-custom-dist-spj-40" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_DSB_CUSTOM.data());
  PredTransTestUtil::testPredTransDist(std::string(SCHEMA_DSB_CUSTOM), "dsb/original/spj/q40_spj.sql",
                                       PARALLEL_DIST_CUSTOM);
}

TEST_CASE ("pred-trans-dsb-custom-dist-spj-50" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_DSB_CUSTOM.data());
  PredTransTestUtil::testPredTransDist(std::string(SCHEMA_DSB_CUSTOM), "dsb/original/spj/q50_spj.sql",
                                       PARALLEL_DIST_CUSTOM);
}

TEST_CASE ("pred-trans-dsb-custom-dist-spj-72" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_DSB_CUSTOM.data());
  PredTransTestUtil::testPredTransDist(std::string(SCHEMA_DSB_CUSTOM), "dsb/original/spj/q72_spj.sql",
                                       PARALLEL_DIST_CUSTOM);
}

TEST_CASE ("pred-trans-dsb-custom-dist-spj-84" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_DSB_CUSTOM.data());
  PredTransTestUtil::testPredTransDist(std::string(SCHEMA_DSB_CUSTOM), "dsb/original/spj/q84_spj.sql",
                                       PARALLEL_DIST_CUSTOM);
}

TEST_CASE ("pred-trans-dsb-custom-dist-spj-85" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_DSB_CUSTOM.data());
  PredTransTestUtil::testPredTransDist(std::string(SCHEMA_DSB_CUSTOM), "dsb/original/spj/q85_spj.sql",
                                       PARALLEL_DIST_CUSTOM);
}

TEST_CASE ("pred-trans-dsb-custom-dist-spj-91" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_DSB_CUSTOM.data());
  PredTransTestUtil::testPredTransDist(std::string(SCHEMA_DSB_CUSTOM), "dsb/original/spj/q91_spj.sql",
                                       PARALLEL_DIST_CUSTOM);
}

TEST_CASE ("pred-trans-dsb-custom-dist-spj-99" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_DSB_CUSTOM.data());
  PredTransTestUtil::testPredTransDist(std::string(SCHEMA_DSB_CUSTOM), "dsb/original/spj/q99_spj.sql",
                                       PARALLEL_DIST_CUSTOM);
}

TEST_CASE ("pred-trans-dsb-custom-dist-spj-100" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_DSB_CUSTOM.data());
  PredTransTestUtil::testPredTransDist(std::string(SCHEMA_DSB_CUSTOM), "dsb/original/spj/q100_spj.sql",
                                       PARALLEL_DIST_CUSTOM);
}

TEST_CASE ("pred-trans-dsb-custom-dist-spj-101" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_DSB_CUSTOM.data());
  PredTransTestUtil::testPredTransDist(std::string(SCHEMA_DSB_CUSTOM), "dsb/original/spj/q101_spj.sql",
                                       PARALLEL_DIST_CUSTOM, true, false);
}

TEST_CASE ("pred-trans-dsb-custom-dist-spj-102" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_DSB_CUSTOM.data());
  PredTransTestUtil::testPredTransDist(std::string(SCHEMA_DSB_CUSTOM), "dsb/original/spj/q102_spj.sql",
                                       PARALLEL_DIST_CUSTOM, true, false);
}

}

TEST_SUITE ("no-pred-trans-dsb-custom-dist-spj" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("no-pred-trans-dsb-custom-dist-spj-13" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_DSB_CUSTOM.data());
  PredTransTestUtil::testPredTransDist(std::string(SCHEMA_DSB_CUSTOM), "dsb/original/spj/q13_spj.sql",
                                       PARALLEL_DIST_CUSTOM, false);
}

TEST_CASE ("no-pred-trans-dsb-custom-dist-spj-18" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_DSB_CUSTOM.data());
  PredTransTestUtil::testPredTransDist(std::string(SCHEMA_DSB_CUSTOM), "dsb/original/spj/q18_spj.sql",
                                       PARALLEL_DIST_CUSTOM, false);
}

TEST_CASE ("no-pred-trans-dsb-custom-dist-spj-19" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_DSB_CUSTOM.data());
  PredTransTestUtil::testPredTransDist(std::string(SCHEMA_DSB_CUSTOM), "dsb/original/spj/q19_spj.sql",
                                       PARALLEL_DIST_CUSTOM, false);
}

TEST_CASE ("no-pred-trans-dsb-custom-dist-spj-25" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_DSB_CUSTOM.data());
  PredTransTestUtil::testPredTransDist(std::string(SCHEMA_DSB_CUSTOM), "dsb/original/spj/q25_spj.sql",
                                       PARALLEL_DIST_CUSTOM, false);
}

TEST_CASE ("no-pred-trans-dsb-custom-dist-spj-27" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_DSB_CUSTOM.data());
  PredTransTestUtil::testPredTransDist(std::string(SCHEMA_DSB_CUSTOM), "dsb/original/spj/q27_spj.sql",
                                       PARALLEL_DIST_CUSTOM, false);
}

TEST_CASE ("no-pred-trans-dsb-custom-dist-spj-40" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_DSB_CUSTOM.data());
  PredTransTestUtil::testPredTransDist(std::string(SCHEMA_DSB_CUSTOM), "dsb/original/spj/q40_spj.sql",
                                       PARALLEL_DIST_CUSTOM, false);
}

TEST_CASE ("no-pred-trans-dsb-custom-dist-spj-50" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_DSB_CUSTOM.data());
  PredTransTestUtil::testPredTransDist(std::string(SCHEMA_DSB_CUSTOM), "dsb/original/spj/q50_spj.sql",
                                       PARALLEL_DIST_CUSTOM, false);
}

TEST_CASE ("no-pred-trans-dsb-custom-dist-spj-72" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_DSB_CUSTOM.data());
  PredTransTestUtil::testPredTransDist(std::string(SCHEMA_DSB_CUSTOM), "dsb/original/spj/q72_spj.sql",
                                       PARALLEL_DIST_CUSTOM, false);
}

TEST_CASE ("no-pred-trans-dsb-custom-dist-spj-84" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_DSB_CUSTOM.data());
  PredTransTestUtil::testPredTransDist(std::string(SCHEMA_DSB_CUSTOM), "dsb/original/spj/q84_spj.sql",
                                       PARALLEL_DIST_CUSTOM, false);
}

TEST_CASE ("no-pred-trans-dsb-custom-dist-spj-85" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_DSB_CUSTOM.data());
  PredTransTestUtil::testPredTransDist(std::string(SCHEMA_DSB_CUSTOM), "dsb/original/spj/q85_spj.sql",
                                       PARALLEL_DIST_CUSTOM, false);
}

TEST_CASE ("no-pred-trans-dsb-custom-dist-spj-91" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_DSB_CUSTOM.data());
  PredTransTestUtil::testPredTransDist(std::string(SCHEMA_DSB_CUSTOM), "dsb/original/spj/q91_spj.sql",
                                       PARALLEL_DIST_CUSTOM, false);
}

TEST_CASE ("no-pred-trans-dsb-custom-dist-spj-99" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_DSB_CUSTOM.data());
  PredTransTestUtil::testPredTransDist(std::string(SCHEMA_DSB_CUSTOM), "dsb/original/spj/q99_spj.sql",
                                       PARALLEL_DIST_CUSTOM, false);
}

TEST_CASE ("no-pred-trans-dsb-custom-dist-spj-100" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_DSB_CUSTOM.data());
  PredTransTestUtil::testPredTransDist(std::string(SCHEMA_DSB_CUSTOM), "dsb/original/spj/q100_spj.sql",
                                       PARALLEL_DIST_CUSTOM, false);
}

TEST_CASE ("no-pred-trans-dsb-custom-dist-spj-101" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_DSB_CUSTOM.data());
  PredTransTestUtil::testPredTransDist(std::string(SCHEMA_DSB_CUSTOM), "dsb/original/spj/q101_spj.sql",
                                       PARALLEL_DIST_CUSTOM, false, false);
}

TEST_CASE ("no-pred-trans-dsb-custom-dist-spj-102" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_DSB_CUSTOM.data());
  PredTransTestUtil::testPredTransDist(std::string(SCHEMA_DSB_CUSTOM), "dsb/modified/spj/q102_spj_nopt.sql",
                                       PARALLEL_DIST_CUSTOM, false);
}

}

TEST_SUITE ("pred-trans-dsb-custom-dist-mb" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("pred-trans-dsb-custom-dist-mb-1" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_DSB_CUSTOM.data());
  PredTransTestUtil::testPredTransDist(std::string(SCHEMA_DSB_CUSTOM), "dsb/original/multi-block/q1.sql",
                                       PARALLEL_DIST_CUSTOM);
}

TEST_CASE ("pred-trans-dsb-custom-dist-mb-10" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_DSB_CUSTOM.data());
  PredTransTestUtil::testPredTransDist(std::string(SCHEMA_DSB_CUSTOM), "dsb/original/multi-block/q10.sql",
                                       PARALLEL_DIST_CUSTOM);
}

TEST_CASE ("pred-trans-dsb-custom-dist-mb-14" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_DSB_CUSTOM.data());
  PredTransTestUtil::testPredTransDist(std::string(SCHEMA_DSB_CUSTOM), "dsb/original/multi-block/q14.sql",
                                       PARALLEL_DIST_CUSTOM);
}

TEST_CASE ("pred-trans-dsb-custom-dist-mb-23" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_DSB_CUSTOM.data());
  PredTransTestUtil::testPredTransDist(std::string(SCHEMA_DSB_CUSTOM), "dsb/original/multi-block/q23.sql",
                                       PARALLEL_DIST_CUSTOM);
}

TEST_CASE ("pred-trans-dsb-custom-dist-mb-30" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_DSB_CUSTOM.data());
  PredTransTestUtil::testPredTransDist(std::string(SCHEMA_DSB_CUSTOM), "dsb/original/multi-block/q30.sql",
                                       PARALLEL_DIST_CUSTOM);
}

TEST_CASE ("pred-trans-dsb-custom-dist-mb-31" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_DSB_CUSTOM.data());
  PredTransTestUtil::testPredTransDist(std::string(SCHEMA_DSB_CUSTOM), "dsb/original/multi-block/q31.sql",
                                       PARALLEL_DIST_CUSTOM);
}

TEST_CASE ("pred-trans-dsb-custom-dist-mb-32" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_DSB_CUSTOM.data());
  PredTransTestUtil::testPredTransDist(std::string(SCHEMA_DSB_CUSTOM), "dsb/original/multi-block/q32.sql",
                                       PARALLEL_DIST_CUSTOM);
}

TEST_CASE ("pred-trans-dsb-custom-dist-mb-38" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_DSB_CUSTOM.data());
  PredTransTestUtil::testPredTransDist(std::string(SCHEMA_DSB_CUSTOM), "dsb/original/multi-block/q38.sql",
                                       PARALLEL_DIST_CUSTOM);
}

TEST_CASE ("pred-trans-dsb-custom-dist-mb-39-0" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_DSB_CUSTOM.data());
  PredTransTestUtil::testPredTransDist(std::string(SCHEMA_DSB_CUSTOM), "dsb/original/multi-block/q39_0.sql",
                                       PARALLEL_DIST_CUSTOM);
}

TEST_CASE ("pred-trans-dsb-custom-dist-mb-39-1" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_DSB_CUSTOM.data());
  PredTransTestUtil::testPredTransDist(std::string(SCHEMA_DSB_CUSTOM), "dsb/original/multi-block/q39_1.sql",
                                       PARALLEL_DIST_CUSTOM);
}

TEST_CASE ("pred-trans-dsb-custom-dist-mb-54" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_DSB_CUSTOM.data());
  PredTransTestUtil::testPredTransDist(std::string(SCHEMA_DSB_CUSTOM), "dsb/original/multi-block/q54.sql",
                                       PARALLEL_DIST_CUSTOM);
}

TEST_CASE ("pred-trans-dsb-custom-dist-mb-58" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_DSB_CUSTOM.data());
  PredTransTestUtil::testPredTransDist(std::string(SCHEMA_DSB_CUSTOM), "dsb/original/multi-block/q58.sql",
                                       PARALLEL_DIST_CUSTOM);
}

TEST_CASE ("pred-trans-dsb-custom-dist-mb-59" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_DSB_CUSTOM.data());
  PredTransTestUtil::testPredTransDist(std::string(SCHEMA_DSB_CUSTOM), "dsb/original/multi-block/q59.sql",
                                       PARALLEL_DIST_CUSTOM);
}

TEST_CASE ("pred-trans-dsb-custom-dist-mb-64" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_DSB_CUSTOM.data());
  PredTransTestUtil::testPredTransDist(std::string(SCHEMA_DSB_CUSTOM), "dsb/original/multi-block/q64.sql",
                                       PARALLEL_DIST_CUSTOM);
}

TEST_CASE ("pred-trans-dsb-custom-dist-mb-65" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_DSB_CUSTOM.data());
  PredTransTestUtil::testPredTransDist(std::string(SCHEMA_DSB_CUSTOM), "dsb/original/multi-block/q65.sql",
                                       PARALLEL_DIST_CUSTOM);
}

TEST_CASE ("pred-trans-dsb-custom-dist-mb-69" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_DSB_CUSTOM.data());
  PredTransTestUtil::testPredTransDist(std::string(SCHEMA_DSB_CUSTOM), "dsb/original/multi-block/q69.sql",
                                       PARALLEL_DIST_CUSTOM);
}

TEST_CASE ("pred-trans-dsb-custom-dist-mb-75" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_DSB_CUSTOM.data());
  PredTransTestUtil::testPredTransDist(std::string(SCHEMA_DSB_CUSTOM), "dsb/original/multi-block/q75.sql",
                                       PARALLEL_DIST_CUSTOM);
}

TEST_CASE ("pred-trans-dsb-custom-dist-mb-80" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_DSB_CUSTOM.data());
  PredTransTestUtil::testPredTransDist(std::string(SCHEMA_DSB_CUSTOM), "dsb/original/multi-block/q80.sql",
                                       PARALLEL_DIST_CUSTOM);
}

TEST_CASE ("pred-trans-dsb-custom-dist-mb-81" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_DSB_CUSTOM.data());
  PredTransTestUtil::testPredTransDist(std::string(SCHEMA_DSB_CUSTOM), "dsb/original/multi-block/q81.sql",
                                       PARALLEL_DIST_CUSTOM);
}

TEST_CASE ("pred-trans-dsb-custom-dist-mb-83" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_DSB_CUSTOM.data());
  PredTransTestUtil::testPredTransDist(std::string(SCHEMA_DSB_CUSTOM), "dsb/original/multi-block/q83.sql",
                                       PARALLEL_DIST_CUSTOM);
}

TEST_CASE ("pred-trans-dsb-custom-dist-mb-87" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_DSB_CUSTOM.data());
  PredTransTestUtil::testPredTransDist(std::string(SCHEMA_DSB_CUSTOM), "dsb/original/multi-block/q87.sql",
                                       PARALLEL_DIST_CUSTOM);
}

TEST_CASE ("pred-trans-dsb-custom-dist-mb-92" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_DSB_CUSTOM.data());
  PredTransTestUtil::testPredTransDist(std::string(SCHEMA_DSB_CUSTOM), "dsb/original/multi-block/q92.sql",
                                       PARALLEL_DIST_CUSTOM);
}

TEST_CASE ("pred-trans-dsb-custom-dist-mb-94" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_DSB_CUSTOM.data());
  PredTransTestUtil::testPredTransDist(std::string(SCHEMA_DSB_CUSTOM), "dsb/original/multi-block/q94.sql",
                                       PARALLEL_DIST_CUSTOM);
}

}

TEST_SUITE ("no-pred-trans-dsb-custom-dist-mb" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("no-pred-trans-dsb-custom-dist-mb-1" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_DSB_CUSTOM.data());
  PredTransTestUtil::testPredTransDist(std::string(SCHEMA_DSB_CUSTOM), "dsb/original/multi-block/q1.sql",
                                       PARALLEL_DIST_CUSTOM, false);
}

TEST_CASE ("no-pred-trans-dsb-custom-dist-mb-10" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_DSB_CUSTOM.data());
  PredTransTestUtil::testPredTransDist(std::string(SCHEMA_DSB_CUSTOM), "dsb/original/multi-block/q10.sql",
                                       PARALLEL_DIST_CUSTOM, false);
}

TEST_CASE ("no-pred-trans-dsb-custom-dist-mb-14" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_DSB_CUSTOM.data());
  PredTransTestUtil::testPredTransDist(std::string(SCHEMA_DSB_CUSTOM), "dsb/original/multi-block/q14.sql",
                                       PARALLEL_DIST_CUSTOM, false);
}

TEST_CASE ("no-pred-trans-dsb-custom-dist-mb-23" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_DSB_CUSTOM.data());
  PredTransTestUtil::testPredTransDist(std::string(SCHEMA_DSB_CUSTOM), "dsb/original/multi-block/q23.sql",
                                       PARALLEL_DIST_CUSTOM, false);
}

TEST_CASE ("no-pred-trans-dsb-custom-dist-mb-30" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_DSB_CUSTOM.data());
  PredTransTestUtil::testPredTransDist(std::string(SCHEMA_DSB_CUSTOM), "dsb/original/multi-block/q30.sql",
                                       PARALLEL_DIST_CUSTOM, false);
}

TEST_CASE ("no-pred-trans-dsb-custom-dist-mb-31" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_DSB_CUSTOM.data());
  PredTransTestUtil::testPredTransDist(std::string(SCHEMA_DSB_CUSTOM), "dsb/original/multi-block/q31.sql",
                                       PARALLEL_DIST_CUSTOM, false);
}

TEST_CASE ("no-pred-trans-dsb-custom-dist-mb-32" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_DSB_CUSTOM.data());
  PredTransTestUtil::testPredTransDist(std::string(SCHEMA_DSB_CUSTOM), "dsb/original/multi-block/q32.sql",
                                       PARALLEL_DIST_CUSTOM, false);
}

TEST_CASE ("no-pred-trans-dsb-custom-dist-mb-38" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_DSB_CUSTOM.data());
  PredTransTestUtil::testPredTransDist(std::string(SCHEMA_DSB_CUSTOM), "dsb/original/multi-block/q38.sql",
                                       PARALLEL_DIST_CUSTOM, false);
}

TEST_CASE ("no-pred-trans-dsb-custom-dist-mb-39-0" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_DSB_CUSTOM.data());
  PredTransTestUtil::testPredTransDist(std::string(SCHEMA_DSB_CUSTOM), "dsb/original/multi-block/q39_0.sql",
                                       PARALLEL_DIST_CUSTOM, false);
}

TEST_CASE ("no-pred-trans-dsb-custom-dist-mb-39-1" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_DSB_CUSTOM.data());
  PredTransTestUtil::testPredTransDist(std::string(SCHEMA_DSB_CUSTOM), "dsb/original/multi-block/q39_1.sql",
                                       PARALLEL_DIST_CUSTOM, false);
}

TEST_CASE ("no-pred-trans-dsb-custom-dist-mb-54" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_DSB_CUSTOM.data());
  PredTransTestUtil::testPredTransDist(std::string(SCHEMA_DSB_CUSTOM), "dsb/original/multi-block/q54.sql",
                                       PARALLEL_DIST_CUSTOM, false);
}

TEST_CASE ("no-pred-trans-dsb-custom-dist-mb-58" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_DSB_CUSTOM.data());
  PredTransTestUtil::testPredTransDist(std::string(SCHEMA_DSB_CUSTOM), "dsb/original/multi-block/q58.sql",
                                       PARALLEL_DIST_CUSTOM, false);
}

TEST_CASE ("no-pred-trans-dsb-custom-dist-mb-59" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_DSB_CUSTOM.data());
  PredTransTestUtil::testPredTransDist(std::string(SCHEMA_DSB_CUSTOM), "dsb/original/multi-block/q59.sql",
                                       PARALLEL_DIST_CUSTOM, false);
}

TEST_CASE ("no-pred-trans-dsb-custom-dist-mb-64" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_DSB_CUSTOM.data());
  PredTransTestUtil::testPredTransDist(std::string(SCHEMA_DSB_CUSTOM), "dsb/original/multi-block/q64.sql",
                                       PARALLEL_DIST_CUSTOM, false);
}

TEST_CASE ("no-pred-trans-dsb-custom-dist-mb-65" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_DSB_CUSTOM.data());
  PredTransTestUtil::testPredTransDist(std::string(SCHEMA_DSB_CUSTOM), "dsb/original/multi-block/q65.sql",
                                       PARALLEL_DIST_CUSTOM, false);
}

TEST_CASE ("no-pred-trans-dsb-custom-dist-mb-69" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_DSB_CUSTOM.data());
  PredTransTestUtil::testPredTransDist(std::string(SCHEMA_DSB_CUSTOM), "dsb/original/multi-block/q69.sql",
                                       PARALLEL_DIST_CUSTOM, false);
}

TEST_CASE ("no-pred-trans-dsb-custom-dist-mb-75" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_DSB_CUSTOM.data());
  PredTransTestUtil::testPredTransDist(std::string(SCHEMA_DSB_CUSTOM), "dsb/original/multi-block/q75.sql",
                                       PARALLEL_DIST_CUSTOM, false);
}

TEST_CASE ("no-pred-trans-dsb-custom-dist-mb-80" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_DSB_CUSTOM.data());
  PredTransTestUtil::testPredTransDist(std::string(SCHEMA_DSB_CUSTOM), "dsb/original/multi-block/q80.sql",
                                       PARALLEL_DIST_CUSTOM, false);
}

TEST_CASE ("no-pred-trans-dsb-custom-dist-mb-81" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_DSB_CUSTOM.data());
  PredTransTestUtil::testPredTransDist(std::string(SCHEMA_DSB_CUSTOM), "dsb/original/multi-block/q81.sql",
                                       PARALLEL_DIST_CUSTOM, false);
}

TEST_CASE ("no-pred-trans-dsb-custom-dist-mb-83" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_DSB_CUSTOM.data());
  PredTransTestUtil::testPredTransDist(std::string(SCHEMA_DSB_CUSTOM), "dsb/original/multi-block/q83.sql",
                                       PARALLEL_DIST_CUSTOM, false);
}

TEST_CASE ("no-pred-trans-dsb-custom-dist-mb-87" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_DSB_CUSTOM.data());
  PredTransTestUtil::testPredTransDist(std::string(SCHEMA_DSB_CUSTOM), "dsb/original/multi-block/q87.sql",
                                       PARALLEL_DIST_CUSTOM, false);
}

TEST_CASE ("no-pred-trans-dsb-custom-dist-mb-92" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_DSB_CUSTOM.data());
  PredTransTestUtil::testPredTransDist(std::string(SCHEMA_DSB_CUSTOM), "dsb/original/multi-block/q92.sql",
                                       PARALLEL_DIST_CUSTOM, false);
}

TEST_CASE ("no-pred-trans-dsb-custom-dist-mb-94" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_DSB_CUSTOM.data());
  PredTransTestUtil::testPredTransDist(std::string(SCHEMA_DSB_CUSTOM), "dsb/original/multi-block/q94.sql",
                                       PARALLEL_DIST_CUSTOM, false);
}

}

TEST_SUITE ("lip-tpch-custom-dist" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("lip-tpch-custom-dist-01" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_TPCH_CUSTOM.data());
  PredTransTestUtil::testLIPDist(std::string(SCHEMA_TPCH_CUSTOM), "tpch/original/01.sql",
                                 PARALLEL_DIST_CUSTOM);
}

TEST_CASE ("lip-tpch-custom-dist-02" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_TPCH_CUSTOM.data());
  PredTransTestUtil::testLIPDist(std::string(SCHEMA_TPCH_CUSTOM), "tpch/original/02.sql",
                                 PARALLEL_DIST_CUSTOM);
}

TEST_CASE ("lip-tpch-custom-dist-03" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_TPCH_CUSTOM.data());
  PredTransTestUtil::testLIPDist(std::string(SCHEMA_TPCH_CUSTOM), "tpch/original/03.sql",
                                 PARALLEL_DIST_CUSTOM);
}

TEST_CASE ("lip-tpch-custom-dist-04" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_TPCH_CUSTOM.data());
  PredTransTestUtil::testLIPDist(std::string(SCHEMA_TPCH_CUSTOM), "tpch/original/04.sql",
                                 PARALLEL_DIST_CUSTOM);
}

// force to use join order 2
TEST_CASE ("lip-tpch-custom-dist-05" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_TPCH_CUSTOM.data());
  PredTransTestUtil::testLIPDist(std::string(SCHEMA_TPCH_CUSTOM), "tpch/modified/05-jo2.sql",
                                 PARALLEL_DIST_CUSTOM, false);
}

TEST_CASE ("lip-tpch-custom-dist-06" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_TPCH_CUSTOM.data());
  PredTransTestUtil::testLIPDist(std::string(SCHEMA_TPCH_CUSTOM), "tpch/original/06.sql",
                                 PARALLEL_DIST_CUSTOM);
}

TEST_CASE ("lip-tpch-custom-dist-07" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_TPCH_CUSTOM.data());
  PredTransTestUtil::testLIPDist(std::string(SCHEMA_TPCH_CUSTOM), "tpch/original/07.sql",
                                 PARALLEL_DIST_CUSTOM);
}

TEST_CASE ("lip-tpch-custom-dist-08" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_TPCH_CUSTOM.data());
  PredTransTestUtil::testLIPDist(std::string(SCHEMA_TPCH_CUSTOM), "tpch/original/08.sql",
                                 PARALLEL_DIST_CUSTOM);
}

TEST_CASE ("lip-tpch-custom-dist-09" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_TPCH_CUSTOM.data());
  PredTransTestUtil::testLIPDist(std::string(SCHEMA_TPCH_CUSTOM), "tpch/original/09.sql",
                                 PARALLEL_DIST_CUSTOM);
}

TEST_CASE ("lip-tpch-custom-dist-10" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_TPCH_CUSTOM.data());
  PredTransTestUtil::testLIPDist(std::string(SCHEMA_TPCH_CUSTOM), "tpch/original/10.sql",
                                 PARALLEL_DIST_CUSTOM);
}

TEST_CASE ("lip-tpch-custom-dist-11" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_TPCH_CUSTOM.data());
  PredTransTestUtil::testLIPDist(std::string(SCHEMA_TPCH_CUSTOM), "tpch/original/11.sql",
                                 PARALLEL_DIST_CUSTOM);
}

TEST_CASE ("lip-tpch-custom-dist-12" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_TPCH_CUSTOM.data());
  PredTransTestUtil::testLIPDist(std::string(SCHEMA_TPCH_CUSTOM), "tpch/original/12.sql",
                                 PARALLEL_DIST_CUSTOM);
}

TEST_CASE ("lip-tpch-custom-dist-13" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_TPCH_CUSTOM.data());
  PredTransTestUtil::testLIPDist(std::string(SCHEMA_TPCH_CUSTOM), "tpch/original/13.sql",
                                 PARALLEL_DIST_CUSTOM);
}

TEST_CASE ("lip-tpch-custom-dist-14" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_TPCH_CUSTOM.data());
  PredTransTestUtil::testLIPDist(std::string(SCHEMA_TPCH_CUSTOM), "tpch/original/14.sql",
                                 PARALLEL_DIST_CUSTOM);
}

TEST_CASE ("lip-tpch-custom-dist-15" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_TPCH_CUSTOM.data());
  PredTransTestUtil::testLIPDist(std::string(SCHEMA_TPCH_CUSTOM), "tpch/original/15.sql",
                                 PARALLEL_DIST_CUSTOM);
}

TEST_CASE ("lip-tpch-custom-dist-16" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_TPCH_CUSTOM.data());
  PredTransTestUtil::testLIPDist(std::string(SCHEMA_TPCH_CUSTOM), "tpch/original/16.sql",
                                 PARALLEL_DIST_CUSTOM);
}

TEST_CASE ("lip-tpch-custom-dist-17" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_TPCH_CUSTOM.data());
  PredTransTestUtil::testLIPDist(std::string(SCHEMA_TPCH_CUSTOM), "tpch/original/17.sql",
                                 PARALLEL_DIST_CUSTOM);
}

TEST_CASE ("lip-tpch-custom-dist-18" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_TPCH_CUSTOM.data());
  PredTransTestUtil::testLIPDist(std::string(SCHEMA_TPCH_CUSTOM), "tpch/original/18.sql",
                                 PARALLEL_DIST_CUSTOM);
}

TEST_CASE ("lip-tpch-custom-dist-19" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_TPCH_CUSTOM.data());
  PredTransTestUtil::testLIPDist(std::string(SCHEMA_TPCH_CUSTOM), "tpch/original/19.sql",
                                 PARALLEL_DIST_CUSTOM);
}

TEST_CASE ("lip-tpch-custom-dist-20" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_TPCH_CUSTOM.data());
  PredTransTestUtil::testLIPDist(std::string(SCHEMA_TPCH_CUSTOM), "tpch/original/20.sql",
                                 PARALLEL_DIST_CUSTOM);
}

TEST_CASE ("lip-tpch-custom-dist-21" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_TPCH_CUSTOM.data());
  PredTransTestUtil::testLIPDist(std::string(SCHEMA_TPCH_CUSTOM), "tpch/original/21.sql",
                                 PARALLEL_DIST_CUSTOM);
}

TEST_CASE ("lip-tpch-custom-dist-22" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_TPCH_CUSTOM.data());
  PredTransTestUtil::testLIPDist(std::string(SCHEMA_TPCH_CUSTOM), "tpch/original/22.sql",
                                 PARALLEL_DIST_CUSTOM);
}

}

TEST_SUITE ("lip-dsb-custom-dist-spj" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("lip-dsb-custom-dist-spj-13" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_DSB_CUSTOM.data());
  PredTransTestUtil::testLIPDist(std::string(SCHEMA_DSB_CUSTOM), "dsb/original/spj/q13_spj.sql",
                                 PARALLEL_DIST_CUSTOM);
}

TEST_CASE ("lip-dsb-custom-dist-spj-18" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_DSB_CUSTOM.data());
  PredTransTestUtil::testLIPDist(std::string(SCHEMA_DSB_CUSTOM), "dsb/original/spj/q18_spj.sql",
                                 PARALLEL_DIST_CUSTOM);
}

TEST_CASE ("lip-dsb-custom-dist-spj-19" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_DSB_CUSTOM.data());
  PredTransTestUtil::testLIPDist(std::string(SCHEMA_DSB_CUSTOM), "dsb/original/spj/q19_spj.sql",
                                 PARALLEL_DIST_CUSTOM);
}

TEST_CASE ("lip-dsb-custom-dist-spj-25" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_DSB_CUSTOM.data());
  PredTransTestUtil::testLIPDist(std::string(SCHEMA_DSB_CUSTOM), "dsb/original/spj/q25_spj.sql",
                                 PARALLEL_DIST_CUSTOM);
}

TEST_CASE ("lip-dsb-custom-dist-spj-27" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_DSB_CUSTOM.data());
  PredTransTestUtil::testLIPDist(std::string(SCHEMA_DSB_CUSTOM), "dsb/original/spj/q27_spj.sql",
                                 PARALLEL_DIST_CUSTOM);
}

TEST_CASE ("lip-dsb-custom-dist-spj-40" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_DSB_CUSTOM.data());
  PredTransTestUtil::testLIPDist(std::string(SCHEMA_DSB_CUSTOM), "dsb/original/spj/q40_spj.sql",
                                 PARALLEL_DIST_CUSTOM);
}

TEST_CASE ("lip-dsb-custom-dist-spj-50" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_DSB_CUSTOM.data());
  PredTransTestUtil::testLIPDist(std::string(SCHEMA_DSB_CUSTOM), "dsb/original/spj/q50_spj.sql",
                                 PARALLEL_DIST_CUSTOM);
}

TEST_CASE ("lip-dsb-custom-dist-spj-72" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_DSB_CUSTOM.data());
  PredTransTestUtil::testLIPDist(std::string(SCHEMA_DSB_CUSTOM), "dsb/original/spj/q72_spj.sql",
                                 PARALLEL_DIST_CUSTOM);
}

TEST_CASE ("lip-dsb-custom-dist-spj-84" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_DSB_CUSTOM.data());
  PredTransTestUtil::testLIPDist(std::string(SCHEMA_DSB_CUSTOM), "dsb/original/spj/q84_spj.sql",
                                 PARALLEL_DIST_CUSTOM);
}

TEST_CASE ("lip-dsb-custom-dist-spj-85" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_DSB_CUSTOM.data());
  PredTransTestUtil::testLIPDist(std::string(SCHEMA_DSB_CUSTOM), "dsb/original/spj/q85_spj.sql",
                                 PARALLEL_DIST_CUSTOM);
}

TEST_CASE ("lip-dsb-custom-dist-spj-91" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_DSB_CUSTOM.data());
  PredTransTestUtil::testLIPDist(std::string(SCHEMA_DSB_CUSTOM), "dsb/original/spj/q91_spj.sql",
                                 PARALLEL_DIST_CUSTOM);
}

TEST_CASE ("lip-dsb-custom-dist-spj-99" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_DSB_CUSTOM.data());
  PredTransTestUtil::testLIPDist(std::string(SCHEMA_DSB_CUSTOM), "dsb/original/spj/q99_spj.sql",
                                 PARALLEL_DIST_CUSTOM);
}

TEST_CASE ("lip-dsb-custom-dist-spj-100" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_DSB_CUSTOM.data());
  PredTransTestUtil::testLIPDist(std::string(SCHEMA_DSB_CUSTOM), "dsb/original/spj/q100_spj.sql",
                                 PARALLEL_DIST_CUSTOM);
}

TEST_CASE ("lip-dsb-custom-dist-spj-101" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_DSB_CUSTOM.data());
  PredTransTestUtil::testLIPDist(std::string(SCHEMA_DSB_CUSTOM), "dsb/original/spj/q101_spj.sql",
                                 PARALLEL_DIST_CUSTOM, false);
}

TEST_CASE ("lip-dsb-custom-dist-spj-102" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_DSB_CUSTOM.data());
  PredTransTestUtil::testLIPDist(std::string(SCHEMA_DSB_CUSTOM), "dsb/modified/spj/q102_spj_nopt.sql",
                                 PARALLEL_DIST_CUSTOM);
}

}

TEST_SUITE ("lip-dsb-custom-dist-mb" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("lip-dsb-custom-dist-mb-1" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_DSB_CUSTOM.data());
  PredTransTestUtil::testLIPDist(std::string(SCHEMA_DSB_CUSTOM), "dsb/original/multi-block/q1.sql",
                                 PARALLEL_DIST_CUSTOM);
}

TEST_CASE ("lip-dsb-custom-dist-mb-10" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_DSB_CUSTOM.data());
  PredTransTestUtil::testLIPDist(std::string(SCHEMA_DSB_CUSTOM), "dsb/original/multi-block/q10.sql",
                                 PARALLEL_DIST_CUSTOM);
}

TEST_CASE ("lip-dsb-custom-dist-mb-14" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_DSB_CUSTOM.data());
  PredTransTestUtil::testLIPDist(std::string(SCHEMA_DSB_CUSTOM), "dsb/original/multi-block/q14.sql",
                                 PARALLEL_DIST_CUSTOM);
}

TEST_CASE ("lip-dsb-custom-dist-mb-23" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_DSB_CUSTOM.data());
  PredTransTestUtil::testLIPDist(std::string(SCHEMA_DSB_CUSTOM), "dsb/original/multi-block/q23.sql",
                                 PARALLEL_DIST_CUSTOM);
}

TEST_CASE ("lip-dsb-custom-dist-mb-30" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_DSB_CUSTOM.data());
  PredTransTestUtil::testLIPDist(std::string(SCHEMA_DSB_CUSTOM), "dsb/original/multi-block/q30.sql",
                                 PARALLEL_DIST_CUSTOM);
}

TEST_CASE ("lip-dsb-custom-dist-mb-31" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_DSB_CUSTOM.data());
  PredTransTestUtil::testLIPDist(std::string(SCHEMA_DSB_CUSTOM), "dsb/original/multi-block/q31.sql",
                                 PARALLEL_DIST_CUSTOM);
}

TEST_CASE ("lip-dsb-custom-dist-mb-32" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_DSB_CUSTOM.data());
  PredTransTestUtil::testLIPDist(std::string(SCHEMA_DSB_CUSTOM), "dsb/original/multi-block/q32.sql",
                                 PARALLEL_DIST_CUSTOM);
}

TEST_CASE ("lip-dsb-custom-dist-mb-38" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_DSB_CUSTOM.data());
  PredTransTestUtil::testLIPDist(std::string(SCHEMA_DSB_CUSTOM), "dsb/original/multi-block/q38.sql",
                                 PARALLEL_DIST_CUSTOM);
}

TEST_CASE ("lip-dsb-custom-dist-mb-39-0" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_DSB_CUSTOM.data());
  PredTransTestUtil::testLIPDist(std::string(SCHEMA_DSB_CUSTOM), "dsb/original/multi-block/q39_0.sql",
                                 PARALLEL_DIST_CUSTOM);
}

TEST_CASE ("lip-dsb-custom-dist-mb-39-1" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_DSB_CUSTOM.data());
  PredTransTestUtil::testLIPDist(std::string(SCHEMA_DSB_CUSTOM), "dsb/original/multi-block/q39_1.sql",
                                 PARALLEL_DIST_CUSTOM);
}

TEST_CASE ("lip-dsb-custom-dist-mb-54" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_DSB_CUSTOM.data());
  PredTransTestUtil::testLIPDist(std::string(SCHEMA_DSB_CUSTOM), "dsb/original/multi-block/q54.sql",
                                 PARALLEL_DIST_CUSTOM);
}

TEST_CASE ("lip-dsb-custom-dist-mb-58" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_DSB_CUSTOM.data());
  PredTransTestUtil::testLIPDist(std::string(SCHEMA_DSB_CUSTOM), "dsb/original/multi-block/q58.sql",
                                 PARALLEL_DIST_CUSTOM);
}

TEST_CASE ("lip-dsb-custom-dist-mb-59" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_DSB_CUSTOM.data());
  PredTransTestUtil::testLIPDist(std::string(SCHEMA_DSB_CUSTOM), "dsb/original/multi-block/q59.sql",
                                 PARALLEL_DIST_CUSTOM);
}

TEST_CASE ("lip-dsb-custom-dist-mb-64" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_DSB_CUSTOM.data());
  PredTransTestUtil::testLIPDist(std::string(SCHEMA_DSB_CUSTOM), "dsb/original/multi-block/q64.sql",
                                 PARALLEL_DIST_CUSTOM);
}

TEST_CASE ("lip-dsb-custom-dist-mb-65" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_DSB_CUSTOM.data());
  PredTransTestUtil::testLIPDist(std::string(SCHEMA_DSB_CUSTOM), "dsb/original/multi-block/q65.sql",
                                 PARALLEL_DIST_CUSTOM);
}

TEST_CASE ("lip-dsb-custom-dist-mb-69" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_DSB_CUSTOM.data());
  PredTransTestUtil::testLIPDist(std::string(SCHEMA_DSB_CUSTOM), "dsb/original/multi-block/q69.sql",
                                 PARALLEL_DIST_CUSTOM);
}

TEST_CASE ("lip-dsb-custom-dist-mb-75" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_DSB_CUSTOM.data());
  PredTransTestUtil::testLIPDist(std::string(SCHEMA_DSB_CUSTOM), "dsb/original/multi-block/q75.sql",
                                 PARALLEL_DIST_CUSTOM);
}

TEST_CASE ("lip-dsb-custom-dist-mb-80" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_DSB_CUSTOM.data());
  PredTransTestUtil::testLIPDist(std::string(SCHEMA_DSB_CUSTOM), "dsb/original/multi-block/q80.sql",
                                 PARALLEL_DIST_CUSTOM);
}

TEST_CASE ("lip-dsb-custom-dist-mb-81" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_DSB_CUSTOM.data());
  PredTransTestUtil::testLIPDist(std::string(SCHEMA_DSB_CUSTOM), "dsb/original/multi-block/q81.sql",
                                 PARALLEL_DIST_CUSTOM);
}

TEST_CASE ("lip-dsb-custom-dist-mb-83" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_DSB_CUSTOM.data());
  PredTransTestUtil::testLIPDist(std::string(SCHEMA_DSB_CUSTOM), "dsb/original/multi-block/q83.sql",
                                 PARALLEL_DIST_CUSTOM);
}

TEST_CASE ("lip-dsb-custom-dist-mb-87" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_DSB_CUSTOM.data());
  PredTransTestUtil::testLIPDist(std::string(SCHEMA_DSB_CUSTOM), "dsb/original/multi-block/q87.sql",
                                 PARALLEL_DIST_CUSTOM);
}

TEST_CASE ("lip-dsb-custom-dist-mb-92" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_DSB_CUSTOM.data());
  PredTransTestUtil::testLIPDist(std::string(SCHEMA_DSB_CUSTOM), "dsb/original/multi-block/q92.sql",
                                 PARALLEL_DIST_CUSTOM);
}

TEST_CASE ("lip-dsb-custom-dist-mb-94" * doctest::skip(false || SKIP_SUITE)) {
  printf("[CUSTOM] on %s\n", SCHEMA_DSB_CUSTOM.data());
  PredTransTestUtil::testLIPDist(std::string(SCHEMA_DSB_CUSTOM), "dsb/original/multi-block/q94.sql",
                                 PARALLEL_DIST_CUSTOM);
}

}

}
