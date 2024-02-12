//
// Created by matt on 24/4/20.
//
//
//#include <doctest/doctest.h>
//
//#include <normal/ssb/query2_1/LocalFileSystemTests.h>
//
//using namespace normal::ssb::query2_1;
//
//#define SKIP_SUITE false
//
//TEST_SUITE ("ssb-query2.1-sf0.01-file" * doctest::skip(SKIP_SUITE)) {
//
//TEST_CASE ("ssb-query2.1-sf0.01-file-par1-partfilter" * doctest::skip(false || SKIP_SUITE)) {
//  auto n = Normal::start();
//  LocalFileSystemTests::partFilter("MFGR#12", "data/ssb-sf0.01", FileType::CSV, 1, true, n);
//  n->stop();
//}
//
//TEST_CASE ("ssb-query2.1-sf0.01-file-par1-join2x" * doctest::skip(false || SKIP_SUITE)) {
//  auto n = Normal::start();
//  LocalFileSystemTests::join2x("AMERICA", "data/ssb-sf0.01", FileType::CSV, 1, true, n);
//  n->stop();
//}
//
//TEST_CASE ("ssb-query2.1-sf0.01-file-par1-join3x" * doctest::skip(false || SKIP_SUITE)) {
//  auto n = Normal::start();
//  LocalFileSystemTests::join3x("AMERICA", "data/ssb-sf0.01", FileType::CSV, 1, true, n);
//  n->stop();
//}
//
//TEST_CASE ("ssb-query2.1-sf0.01-file-par1-join" * doctest::skip(false || SKIP_SUITE)) {
//  auto n = Normal::start();
//  LocalFileSystemTests::join("MFGR#12", "AMERICA", "data/ssb-sf0.01", FileType::CSV, 1, true, n);
//  n->stop();
//}
//
//}
