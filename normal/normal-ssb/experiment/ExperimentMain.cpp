//
// Created by Yifei Yang on 7/7/20.
//

#define DOCTEST_CONFIG_IMPLEMENT
#include <doctest/doctest.h>
#include "normal/ssb/Globals.h"
#include "Tests.h"
#include "MathModelTest.h"

using namespace normal::ssb;

#define BACKWARD_HAS_BFD 1
#include <backward.hpp>
#include <normal/connector/MiniCatalogue.h>

backward::SignalHandling sh;

const char* getCurrentTestName() { return doctest::detail::g_cs->currentTest->m_name; }
const char* getCurrentTestSuiteName() { return doctest::detail::g_cs->currentTest->m_test_suite; }

int main(int argc, char **argv) {

  normal::connector::defaultMiniCatalogue = normal::connector::MiniCatalogue::defaultMiniCatalogue("pushdowndb", "ssb-sf100-sortlineorder/csv/");
  // math model test
  if (std::string(argv[1]) == "-m") {
    auto networkLimit = (size_t) (atof(argv[2]) * 1024 * 1024 * 1024 / 8);
    auto numRuns = atoi(argv[3]);
    MathModelTest mathModelTest(networkLimit, numRuns);
    mathModelTest.runTest();
  }
  // means we are specifying a different defaultMiniCatalogue directoy to use
  else if (std::string(argv[1]) == "-d") {
    std::string dirPrefix = argv[2];
    auto cacheSize = (size_t) (atof(argv[3]) * 1024 * 1024 * 1024);
    auto modeType = atoi(argv[4]);
    auto cachingPolicyType = atoi(argv[5]);
    SPDLOG_INFO("Cache size: {}", cacheSize);
    SPDLOG_INFO("Mode type: {}", modeType);
    SPDLOG_INFO("CachingPolicy type: {}", cachingPolicyType);
    if (argc < 7) {
      mainTest(cacheSize, modeType, cachingPolicyType, dirPrefix, false);
    } else {
      bool writeResults = atoi(argv[6]);
      mainTest(cacheSize, modeType, cachingPolicyType, dirPrefix, writeResults);
    }
  }

  // concurrent request test
  else if (std::string(argv[1]) == "-cS") {
    auto partitionNum = (int) (atoi(argv[2]));
    concurrentSelectTest(partitionNum);
  } else if (std::string(argv[1]) == "-cG") {
    auto partitionNum = (int) (atoi(argv[2]));
    concurrentGetTest(partitionNum);
  }

    // main test
  else {
    std::string dirPrefix = "ssb-sf100-sortlineorder/csv/";
    auto cacheSize = (size_t) (atof(argv[1]) * 1024 * 1024 * 1024);
    auto modeType = atoi(argv[2]);
    auto cachingPolicyType = atoi(argv[3]);
    SPDLOG_INFO("Cache size: {}", cacheSize);
    SPDLOG_INFO("Mode type: {}", modeType);
    SPDLOG_INFO("CachingPolicy type: {}", cachingPolicyType);
    if (argc < 5) {
      mainTest(cacheSize, modeType, cachingPolicyType, dirPrefix, false);
    } else {
      bool writeResults = atoi(argv[4]);
      mainTest(cacheSize, modeType, cachingPolicyType, dirPrefix, writeResults);
    }
  }

  return 0;
}
