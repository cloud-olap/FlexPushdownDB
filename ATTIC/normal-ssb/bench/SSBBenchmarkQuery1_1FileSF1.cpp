//
// Created by matt on 5/8/20.
//

//
//#include <doctest/doctest.h>
//#include <nanobench.h>
//
//#include <normal/core/ATTIC/Normal.h>
//#include <normal/ssb/query1_1/LocalFileSystemTests.h>
//#include <normal/ssb/TestUtil.h>
//
//using namespace normal::core;
//using namespace normal::ssb;
//
//#define SKIP_SUITE false
//
//inline static constexpr auto SF = "1";
//
//TEST_SUITE ("ssb-benchmark-query1.1-file-sf1" * doctest::skip(SKIP_SUITE)) {
//
//TEST_CASE ("ssb-benchmark-query1.1-file-sf1-p32-cacheNone" * doctest::skip(false || SKIP_SUITE)) {
//  ankerl::nanobench::Config().minEpochIterations(1).run(
//	  getCurrentTestName(), [&] {
//		auto n = Normal::start();
//		LocalFileSystemTests::full2(1992, 2, 25, fmt::format("data/ssb-sf{}", SF), FileType::CSV, 32, 1, false, n);
//		n->stop();
//	  });
//}
//
//TEST_CASE ("ssb-benchmark-query1.1-file-sf1-p32-cacheWarm" * doctest::skip(false || SKIP_SUITE)) {
//
//  auto n = Normal::start();
//
//  // Warm cache
//  LocalFileSystemTests::full2(1992, 2, 25, fmt::format("data/ssb-sf{}", SF), FileType::CSV, 32, 1, false, n);
//
//  ankerl::nanobench::Config().minEpochIterations(1).run(
//	  getCurrentTestName(), [&] {
//		LocalFileSystemTests::full2(1992, 2, 25, fmt::format("data/ssb-sf{}", SF), FileType::CSV, 32, 1, false, n);
//	  });
//
//  n->stop();
//}
//
//}
