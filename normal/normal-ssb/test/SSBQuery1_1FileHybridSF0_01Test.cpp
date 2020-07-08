//
// Created by matt on 24/4/20.
//

#include <doctest/doctest.h>

#include <normal/ssb/query1_1/LocalFileSystemTests.h>

using namespace normal::ssb;

#define SKIP_SUITE false

TEST_SUITE ("ssb-query1.1-file-hybrid-sf0.01" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("date-scan-par1-iter2" * doctest::skip(false || SKIP_SUITE)) {
  LocalFileSystemTests::dateScanMulti("data/ssb-sf0.01", 1, 2, true);
}

}
