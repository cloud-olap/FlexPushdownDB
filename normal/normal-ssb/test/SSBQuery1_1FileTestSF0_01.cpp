//
// Created by matt on 24/4/20.
//

#include <doctest/doctest.h>

#include <normal/ssb/query1_1/LocalFileSystemTests.h>

using namespace normal::ssb;

#define SKIP_SUITE true

TEST_SUITE ("ssb-query1.1-file-sf0.01" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("date-scan-par1" * doctest::skip(false || SKIP_SUITE)) {
  LocalFileSystemTests::dateScan("data/ssb-sf0.01", 1, true);
}

TEST_CASE ("date-scan-par2" * doctest::skip(false || SKIP_SUITE)) {
  LocalFileSystemTests::dateScan("data/ssb-sf0.01", 2, true);
}

TEST_CASE ("lineorder-scan-par1" * doctest::skip(false || SKIP_SUITE)) {
  LocalFileSystemTests::lineOrderScan("data/ssb-sf0.01", 1, true);
}

TEST_CASE ("lineorder-scan-par2" * doctest::skip(false || SKIP_SUITE)) {
  LocalFileSystemTests::lineOrderScan("data/ssb-sf0.01", 2, true);
}

TEST_CASE ("date-filter-par1" * doctest::skip(false || SKIP_SUITE)) {
  LocalFileSystemTests::dateFilter(1992, "data/ssb-sf0.01", 1, true);
}

TEST_CASE ("date-filter-par2" * doctest::skip(false || SKIP_SUITE)) {
  LocalFileSystemTests::dateFilter(1992, "data/ssb-sf0.01", 2, true);
}

TEST_CASE ("lineorder-filter-par1" * doctest::skip(false || SKIP_SUITE)) {
  LocalFileSystemTests::lineOrderFilter(2, 25, "data/ssb-sf0.01", 1, true);
}

TEST_CASE ("lineorder-filter-par2" * doctest::skip(false || SKIP_SUITE)) {
  LocalFileSystemTests::lineOrderFilter(2, 25, "data/ssb-sf0.01", 2, true);
}

TEST_CASE ("join-par1" * doctest::skip(false || SKIP_SUITE)) {
  LocalFileSystemTests::join(1992, 2, 25, "data/ssb-sf0.01", 1, true);
}

TEST_CASE ("join-par2" * doctest::skip(false || SKIP_SUITE)) {
  LocalFileSystemTests::join(1992, 2, 25, "data/ssb-sf0.01", 2, true);
}

TEST_CASE ("full-par1" * doctest::skip(false || SKIP_SUITE)) {
  LocalFileSystemTests::full(1992, 2, 25, "data/ssb-sf0.01", 1, true);
}

TEST_CASE ("full-par2" * doctest::skip(false || SKIP_SUITE)) {
  LocalFileSystemTests::full(1992, 2, 25, "data/ssb-sf0.01", 2, true);
}

}
