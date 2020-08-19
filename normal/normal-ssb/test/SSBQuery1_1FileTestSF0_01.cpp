//
// Created by matt on 24/4/20.
//

#include <doctest/doctest.h>

#include <normal/ssb/query1_1/LocalFileSystemTests.h>

using namespace normal::ssb;

#define SKIP_SUITE false

TEST_SUITE ("ssb-query1.1-file-csv-sf0.01" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("ssb-query1.1-file-csv-sf0.01-par1-date-scan" * doctest::skip(false || SKIP_SUITE)) {
  auto n = Normal::start();
  LocalFileSystemTests::dateScan("data/ssb-sf0.01", FileType::CSV, 1, 1, true, n);
  n->stop();

	  CHECK_EQ(::arrow::default_memory_pool()->bytes_allocated(), 0);
}

TEST_CASE ("ssb-query1.1-file-csv-sf0.01-par2-date-scan" * doctest::skip(false || SKIP_SUITE)) {
  auto n = Normal::start();
  LocalFileSystemTests::dateScan("data/ssb-sf0.01", FileType::CSV, 2, 1, true, n);
  n->stop();

	  CHECK_EQ(::arrow::default_memory_pool()->bytes_allocated(), 0);
}

TEST_CASE ("ssb-query1.1-file-csv-sf0.01-par1-lineorder-scan" * doctest::skip(false || SKIP_SUITE)) {
  auto n = Normal::start();
  LocalFileSystemTests::lineOrderScan("data/ssb-sf0.01", FileType::CSV, 1, 1, true, n);
  n->stop();

	  CHECK_EQ(::arrow::default_memory_pool()->bytes_allocated(), 0);
}

TEST_CASE ("ssb-query1.1-file-csv-sf0.01-par2-lineorder-scan" * doctest::skip(false || SKIP_SUITE)) {
  auto n = Normal::start();
  LocalFileSystemTests::lineOrderScan("data/ssb-sf0.01", FileType::CSV, 2, 1, true, n);
  n->stop();

	  CHECK_EQ(::arrow::default_memory_pool()->bytes_allocated(), 0);
}

TEST_CASE ("ssb-query1.1-file-csv-sf0.01-par1-date-filter" * doctest::skip(false || SKIP_SUITE)) {
  auto n = Normal::start();
  LocalFileSystemTests::dateFilter(1992, "data/ssb-sf0.01", FileType::CSV, 1, true, n);
  n->stop();

	  CHECK_EQ(::arrow::default_memory_pool()->bytes_allocated(), 0);
}

TEST_CASE ("ssb-query1.1-file-csv-sf0.01-par2-date-filter" * doctest::skip(false || SKIP_SUITE)) {
  auto n = Normal::start();
  LocalFileSystemTests::dateFilter(1992, "data/ssb-sf0.01", FileType::CSV, 2, true, n);
  n->stop();

	  CHECK_EQ(::arrow::default_memory_pool()->bytes_allocated(), 0);
}

TEST_CASE ("ssb-query1.1-file-csv-sf0.01-par1-lineorder-filter" * doctest::skip(false || SKIP_SUITE)) {
  auto n = Normal::start();
  LocalFileSystemTests::lineOrderFilter(2, 25, "data/ssb-sf0.01", FileType::CSV, 1, true, n);
  n->stop();

	  CHECK_EQ(::arrow::default_memory_pool()->bytes_allocated(), 0);
}

TEST_CASE ("ssb-query1.1-file-csv-sf0.01-par2-lineorder-filter" * doctest::skip(false || SKIP_SUITE)) {
  auto n = Normal::start();
  LocalFileSystemTests::lineOrderFilter(2, 25, "data/ssb-sf0.01", FileType::CSV, 2, true, n);
  n->stop();

	  CHECK_EQ(::arrow::default_memory_pool()->bytes_allocated(), 0);
}

TEST_CASE ("ssb-query1.1-file-csv-sf0.01-par1-join" * doctest::skip(false || SKIP_SUITE)) {
  auto n = Normal::start();
  LocalFileSystemTests::join(1992, 2, 25, "data/ssb-sf0.01", FileType::CSV, 1, true, n);
  n->stop();

	  CHECK_EQ(::arrow::default_memory_pool()->bytes_allocated(), 0);
}

TEST_CASE ("ssb-query1.1-file-csv-sf0.01-par2-join" * doctest::skip(false || SKIP_SUITE)) {
  auto n = Normal::start();
  LocalFileSystemTests::join(1992, 2, 25, "data/ssb-sf0.01", FileType::CSV, 2, true, n);
  n->stop();

	  CHECK_EQ(::arrow::default_memory_pool()->bytes_allocated(), 0);
}

TEST_CASE ("ssb-query1.1-file-csv-sf0.01-par1-full" * doctest::skip(false || SKIP_SUITE)) {
  auto n = Normal::start();
  LocalFileSystemTests::full2(1992, 2, 25, "data/ssb-sf0.01", FileType::CSV, 1, 1, true, n);
  n->stop();

	  CHECK_EQ(::arrow::default_memory_pool()->bytes_allocated(), 0);
}

TEST_CASE ("ssb-query1.1-file-csv-sf0.01-par2-full" * doctest::skip(false || SKIP_SUITE)) {
  auto n = Normal::start();
  LocalFileSystemTests::full2(1992, 2, 25, "data/ssb-sf0.01", FileType::CSV, 2, 1, true, n);
  n->stop();

	  CHECK_EQ(::arrow::default_memory_pool()->bytes_allocated(), 0);
}

//TEST_CASE ("bloom-par1" * doctest::skip(false || SKIP_SUITE)) {
//  auto n = Normal::start();
//  LocalFileSystemTests::bloom(1992, 2, 25, "data/ssb-sf0.01",FileType::CSV,  1, true, n);
//  n->stop();
//}
//
//TEST_CASE ("bloom-par2" * doctest::skip(false || SKIP_SUITE)) {
//  auto n = Normal::start();
//  LocalFileSystemTests::bloom(1992, 2, 25, "data/ssb-sf0.01",FileType::CSV,  2, true, n);
//  n->stop();
//}

}
