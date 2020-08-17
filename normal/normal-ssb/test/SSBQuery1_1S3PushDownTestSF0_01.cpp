//
// Created by matt on 24/4/20.
//

#include <doctest/doctest.h>

#include <normal/ssb/query1_1/S3SelectTests.h>

using namespace normal::ssb;

#define SKIP_SUITE true

TEST_SUITE ("ssb-query1.1-s3-pushdown-sf0.01" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("full-par1" * doctest::skip(false || SKIP_SUITE)) {
  auto n = Normal::start();
  S3SelectTests::fullPushDown(1992, 2, 25, "ssb-sf0.01", "data/ssb-sf0.01", 1, true, n);
  n->stop();
}

TEST_CASE ("full-par2" * doctest::skip(false || SKIP_SUITE)) {
  auto n = Normal::start();
  S3SelectTests::fullPushDown(1992, 2, 25, "ssb-sf0.01", "data/ssb-sf0.01", 2, true, n);
  n->stop();
}

}
