//
// Created by matt on 12/8/20.
//
//
//#include <doctest/doctest.h>
//
//#include <normal/ssb/query1_1/LocalFileSystemTests.h>
//
//using namespace normal::ssb;
//
//TEST_SUITE ("ssb-query1.1-file-parquet-sf0.01") {
//
//TEST_CASE ("ssb-query1.1-file-parquet-sf0.01-par1-date-scan" * doctest::skip(false)) {
//  auto n = Normal::start();
//  LocalFileSystemTests::dateScan("data/ssb-sf0.01", FileType::Parquet, 1, 1, true, n);
//  n->stop();
//}
//
//TEST_CASE ("ssb-query1.1-file-parquet-sf0.01-par1-lineorder-scan" * doctest::skip(false)) {
//  auto n = Normal::start();
//  LocalFileSystemTests::lineOrderScan("data/ssb-sf0.01", FileType::Parquet, 1, 1, true, n);
//  n->stop();
//}
//
//TEST_CASE ("ssb-query1.1-file-parquet-sf0.01-par1-full" * doctest::skip(false)) {
//  auto n = Normal::start();
//  LocalFileSystemTests::full2(1992, 2, 25, "data/ssb-sf0.01", FileType::Parquet, 1, 1, true, n);
//  n->stop();
//}
//
//}