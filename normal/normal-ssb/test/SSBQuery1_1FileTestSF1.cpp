//
// Created by matt on 24/4/20.
//

#include <doctest/doctest.h>

#include <normal/ssb/query1_1/LocalFileSystemTests.h>

using namespace normal::ssb;

#define SKIP_SUITE true

/**
 * NOTE: SQLite cannot execute queries on lineorder on sf=1, too big
 */
TEST_SUITE ("ssb-query1.1-file-sf1" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("date-scan-par1" * doctest::skip(false || SKIP_SUITE)) {
  LocalFileSystemTests::dateScan("data/ssb-sf1", 1, 1, true);
}

TEST_CASE ("date-scan-par2" * doctest::skip(false || SKIP_SUITE)) {
  LocalFileSystemTests::dateScan("data/ssb-sf1", 2, 1, true);
}

TEST_CASE ("lineorder-scan-par1" * doctest::skip(false || SKIP_SUITE)) {
  LocalFileSystemTests::lineOrderScan("data/ssb-sf1", 1, false);
}

TEST_CASE ("lineorder-scan-par2" * doctest::skip(false || SKIP_SUITE)) {
  LocalFileSystemTests::lineOrderScan("data/ssb-sf1", 2, false);
}

TEST_CASE ("date-filter-par1" * doctest::skip(false || SKIP_SUITE)) {
  LocalFileSystemTests::dateFilter(1992, "data/ssb-sf1", 1, true);
}

TEST_CASE ("date-filter-par2" * doctest::skip(false || SKIP_SUITE)) {
  LocalFileSystemTests::dateFilter(1992, "data/ssb-sf1", 2, true);
}

TEST_CASE ("lineorder-filter-par1" * doctest::skip(false || SKIP_SUITE)) {
  LocalFileSystemTests::lineOrderFilter(2, 25, "data/ssb-sf1", 1, false);
}

TEST_CASE ("lineorder-filter-par2" * doctest::skip(false || SKIP_SUITE)) {
  LocalFileSystemTests::lineOrderFilter(2, 25, "data/ssb-sf1", 2, false);
}

TEST_CASE ("join-par8" * doctest::skip(false || SKIP_SUITE)) {
  LocalFileSystemTests::join(1992, 2, 25, "data/ssb-sf1", 8, false);
}

TEST_CASE ("full-par8" * doctest::skip(false || SKIP_SUITE)) {
  LocalFileSystemTests::full(1992, 2, 25, "data/ssb-sf1", 8, false);
}

}
