//
// Created by matt on 24/4/20.
//
//
//#include <doctest/doctest.h>
//
//#include <normal/ssb/query1_1/S3SelectTests.h>
//
//using namespace normal::ssb;
//
//#define SKIP_SUITE false
//
//TEST_SUITE ("ssb-query1.1-s3-hybrid-csv-sf0.01" * doctest::skip(SKIP_SUITE)) {
//
//TEST_CASE ("ssb-query1.1-s3-hybrid-csv-sf0.01-date-scan-par1-iter2" * doctest::skip(false || SKIP_SUITE)) {
//  auto n = Normal::start();
//  S3SelectTests::hybridDateFilter(1992, "ssb-sf0.01", "data/ssb-sf0.01", FileType::CSV, 1, 2, true, n);
//  n->stop();
//}
//
//TEST_CASE ("ssb-query1.1-s3-hybrid-csv-sf0.01-full-par1" * doctest::skip(false || SKIP_SUITE)) {
//  auto n = Normal::start();
//  S3SelectTests::hybrid(1992, 2, 25, "ssb-sf0.01", "data/ssb-sf0.01", FileType::CSV, 1, true, n);
//  n->stop();
//}
//
//TEST_CASE ("ssb-query1.1-s3-hybrid-csv-sf0.01-full-par2" * doctest::skip(false || SKIP_SUITE)) {
//  auto n = Normal::start();
//  S3SelectTests::hybrid(1992, 2, 25, "ssb-sf0.01", "data/ssb-sf0.01", FileType::CSV, 2, true, n);
//  n->stop();
//}
//
//}
