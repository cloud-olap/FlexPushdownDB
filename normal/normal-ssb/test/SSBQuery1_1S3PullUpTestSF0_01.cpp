//
// Created by matt on 24/4/20.
//

#include <doctest/doctest.h>

#include <normal/ssb/query1_1/S3SelectTests.h>

using namespace normal::ssb;

#define SKIP_SUITE true

TEST_SUITE ("ssb-query1.1-s3-pullup-sf0.01" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("date-scan-par1" * doctest::skip(false || SKIP_SUITE)) {
  auto n = Normal::start();
  S3SelectTests::dateScan("ssb-sf0.01", "data/ssb-sf0.01", 1, 1, true, n);
  n->stop();
}

TEST_CASE ("date-scan-par2" * doctest::skip(false || SKIP_SUITE)) {
  auto n = Normal::start();
  S3SelectTests::dateScan("ssb-sf0.01", "data/ssb-sf0.01", 2, 1, true, n);
  n->stop();
}

TEST_CASE ("lineorder-scan-par1" * doctest::skip(false || SKIP_SUITE)) {
  auto n = Normal::start();
  S3SelectTests::lineOrderScan("ssb-sf0.01", "data/ssb-sf0.01", 1, true, n);
  n->stop();
}

TEST_CASE ("lineorder-scan-par2" * doctest::skip(false || SKIP_SUITE)) {
  auto n = Normal::start();
  S3SelectTests::lineOrderScan("ssb-sf0.01", "data/ssb-sf0.01", 2, true, n);
  n->stop();
}

TEST_CASE ("date-filter-par1" * doctest::skip(false || SKIP_SUITE)) {
  auto n = Normal::start();
  S3SelectTests::dateFilter(1992, "ssb-sf0.01", "data/ssb-sf0.01", 1, true, n);
  n->stop();
}

TEST_CASE ("date-filter-par2" * doctest::skip(false || SKIP_SUITE)) {
  auto n = Normal::start();
  S3SelectTests::dateFilter(1992, "ssb-sf0.01", "data/ssb-sf0.01", 2, true, n);
  n->stop();
}

TEST_CASE ("lineorder-filter-par1" * doctest::skip(false || SKIP_SUITE)) {
  auto n = Normal::start();
  S3SelectTests::lineOrderFilter(2, 25, "ssb-sf0.01", "data/ssb-sf0.01", 1, true, n);
  n->stop();
}

TEST_CASE ("lineorder-filter-par2" * doctest::skip(false || SKIP_SUITE)) {
  auto n = Normal::start();
  S3SelectTests::lineOrderFilter(2, 25, "ssb-sf0.01", "data/ssb-sf0.01", 2, true, n);
  n->stop();
}

TEST_CASE ("join-par1" * doctest::skip(false || SKIP_SUITE)) {
  auto n = Normal::start();
  S3SelectTests::join(1992, 2, 25, "ssb-sf0.01", "data/ssb-sf0.01", 1, true, n);
  n->stop();
}

TEST_CASE ("join-par2" * doctest::skip(false || SKIP_SUITE)) {
  auto n = Normal::start();
  S3SelectTests::join(1992, 2, 25, "ssb-sf0.01", "data/ssb-sf0.01", 2, true, n);
  n->stop();
}

TEST_CASE ("full-par1" * doctest::skip(false || SKIP_SUITE)) {
  auto n = Normal::start();
  S3SelectTests::full(1992, 2, 25, "ssb-sf0.01", "data/ssb-sf0.01", 1, true, n);
  n->stop();
}

TEST_CASE ("full-par2" * doctest::skip(false || SKIP_SUITE)) {
  auto n = Normal::start();
  S3SelectTests::full(1992, 2, 25, "ssb-sf0.01", "data/ssb-sf0.01", 2, true, n);
  n->stop();
}

}
