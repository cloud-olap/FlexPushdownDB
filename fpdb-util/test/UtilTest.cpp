//
// Created by Yifei Yang on 1/14/22.
//

#include <fpdb/util/Util.h>
#include "Globals.h"
#include <doctest/doctest.h>

using namespace fpdb::util;

#define SKIP_SUITE false

TEST_SUITE ("util" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("util-getLocalIp" * doctest::skip(false || SKIP_SUITE)) {
  auto expLocalIp = getLocalIp();
  assert(expLocalIp.has_value());
  SPDLOG_DEBUG("localIp: {}", *expLocalIp);
}

}
