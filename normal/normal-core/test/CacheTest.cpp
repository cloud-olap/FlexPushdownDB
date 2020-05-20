//
// Created by matt on 20/5/20.
//

#include <memory>

#include <doctest/doctest.h>

#include <normal/connector/local-fs/LocalFilePartition.h>
#include <normal/tuple/TupleSet2.h>

using namespace normal::tuple;

namespace normal::cache::test {

#define SKIP_SUITE false

TEST_SUITE ("cache" * doctest::skip(SKIP_SUITE)) {

TEST_CASE ("load" * doctest::skip(false || SKIP_SUITE)) {
}

}

}