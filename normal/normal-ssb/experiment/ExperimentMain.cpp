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

backward::SignalHandling sh;

const char* getCurrentTestName() { return doctest::detail::g_cs->currentTest->m_name; }
const char* getCurrentTestSuiteName() { return doctest::detail::g_cs->currentTest->m_test_suite; }

int main(int argc, char **argv) {

  // math model test
  if (std::string(argv[1]) == "-m") {
    auto networkLimit = (size_t) (atof(argv[2]) * 1024 * 1024 * 1024 / 8);
    mathModelTest(networkLimit);
  }

  // main test
  else {
    auto cacheSize = (size_t) (atof(argv[1]) * 1024 * 1024 * 1024);
    auto modeType = atoi(argv[2]);
    auto cachingPolicyType = atoi(argv[3]);
    SPDLOG_INFO("Cache size: {}", cacheSize);
    SPDLOG_INFO("Mode type: {}", modeType);
    SPDLOG_INFO("CachingPolicy type: {}", cachingPolicyType);
    if (argc < 5) {
      mainTest(cacheSize, modeType, cachingPolicyType, false);
    } else {
      bool writeResults = atoi(argv[4]);
      mainTest(cacheSize, modeType, cachingPolicyType, writeResults);
    }
  }

  return 0;
}
