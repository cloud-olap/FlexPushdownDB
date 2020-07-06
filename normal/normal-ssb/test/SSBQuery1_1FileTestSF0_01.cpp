//
// Created by matt on 24/4/20.
//

#include <doctest/doctest.h>

#include <normal/ssb/query1_1/Tests.h>

using namespace normal::ssb;

#define SKIP_SUITE false

TEST_SUITE ("ssb-benchmark-query1.1-file-sf0.01" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("date-scan" * doctest::skip(false || SKIP_SUITE)) {
  Tests::dateScan("data/ssb-sf0.01", 1, true);
  Tests::dateScan("data/ssb-sf0.01", 2, true);
}

TEST_CASE ("lineorder-scan" * doctest::skip(false || SKIP_SUITE)) {
  Tests::lineOrderScan("data/ssb-sf0.01", 1, true);
  Tests::lineOrderScan("data/ssb-sf0.01", 2, true);
}

TEST_CASE ("date-filter" * doctest::skip(false || SKIP_SUITE)) {
  Tests::dateFilter(1992, "data/ssb-sf0.01", 1, true);
  Tests::dateFilter(1992, "data/ssb-sf0.01", 2, true);
}

TEST_CASE ("lineorder-filter" * doctest::skip(false || SKIP_SUITE)) {
  Tests::lineOrderFilter(2, 25, "data/ssb-sf0.01", 1, true);
  Tests::lineOrderFilter(2, 25, "data/ssb-sf0.01", 2, true);
}

TEST_CASE ("join" * doctest::skip(false || SKIP_SUITE)) {
  Tests::join(1992, 2, 25, "data/ssb-sf0.01", 1, true);
  Tests::join(1992, 2, 25, "data/ssb-sf0.01", 2, true);
}

TEST_CASE ("aggregate" * doctest::skip(false || SKIP_SUITE)) {
  Tests::full(1992, 2, 25, "data/ssb-sf0.01", 1, true);
  Tests::full(1992, 2, 25, "data/ssb-sf0.01", 2, true);
}

}
