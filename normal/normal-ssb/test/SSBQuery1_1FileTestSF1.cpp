//
// Created by matt on 24/4/20.
//

#include <doctest/doctest.h>

#include <normal/ssb/query1_1/Tests.h>

using namespace normal::ssb;

#define SKIP_SUITE true

TEST_SUITE ("ssb-benchmark-query1.1-file-sf1" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("date-scan" * doctest::skip(false || SKIP_SUITE)) {
  Tests::dateScan("data/ssb-sf1", 1, true);
  Tests::dateScan("data/ssb-sf1", 2, true);
}

TEST_CASE ("lineorder-scan" * doctest::skip(false || SKIP_SUITE)) {
  Tests::lineOrderScan("data/ssb-sf1", 1, true);
  Tests::lineOrderScan("data/ssb-sf1", 2, true);
}

TEST_CASE ("date-filter" * doctest::skip(false || SKIP_SUITE)) {
  Tests::dateFilter(1992, "data/ssb-sf1", 1, true);
  Tests::dateFilter(1992, "data/ssb-sf1", 2, true);
}

TEST_CASE ("lineorder-filter" * doctest::skip(false || SKIP_SUITE)) {
  Tests::lineOrderFilter(2, 25, "data/ssb-sf1", 1, true);
  Tests::lineOrderFilter(2, 25, "data/ssb-sf1", 2, true);
}

/**
 * NOTE: SQLite cannot execute joins on sf=1, too big
 */
TEST_CASE ("join" * doctest::skip(false || SKIP_SUITE)) {
  Tests::join(1992, 2, 25, "data/ssb-sf1", 8, false);
}

/**
 * NOTE: SQLite cannot execute joins on sf=1, too big
 */
TEST_CASE ("aggregate" * doctest::skip(false || SKIP_SUITE)) {
  Tests::full(1992, 2, 25, "data/ssb-sf1", 8, false);
}

}
