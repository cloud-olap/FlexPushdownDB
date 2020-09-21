//
// Created by Yifei Yang on 7/7/20.
//

#define DOCTEST_CONFIG_IMPLEMENT
#include <doctest/doctest.h>
#include "normal/ssb/Globals.h"

#define BACKWARD_HAS_BFD 1
#include <backward.hpp>

backward::SignalHandling sh;

const char* getCurrentTestName() { return doctest::detail::g_cs->currentTest->m_name; }
const char* getCurrentTestSuiteName() { return doctest::detail::g_cs->currentTest->m_test_suite; }

int main(int argc, char **argv) {

  spdlog::set_level(spdlog::level::debug);
  spdlog::set_pattern("[%H:%M:%S.%e] [thread %t] [%! (%s:%#)] [%l]  %v");

  doctest::Context context;

  context.applyCommandLine(argc, argv);
  int rc = context.run();

  if (context.shouldExit()) // important - query flags (and --exit) rely on the user doing this
    return rc;

  return rc;
}
