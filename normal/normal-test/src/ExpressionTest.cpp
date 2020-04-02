//
// Created by matt on 2/4/20.
//

#include <doctest/doctest.h>
#include <normal/core/expression/Literal.h>
#include <normal/core/expression/Add.h>
#include <normal/core/expression/Subtract.h>
#include <normal/core/expression/Multiply.h>
#include <normal/core/expression/Divide.h>

#include "Globals.h"

using namespace normal::core::expression;

TEST_CASE ("Literal" * doctest::skip(false)) {
  auto value = lit(10)->eval();
      CHECK_EQ(value, 10);
}

TEST_CASE ("Add" * doctest::skip(false)) {
  auto value = plus(lit(10), lit(20))->eval();
      CHECK_EQ(value, 30);
}

TEST_CASE ("Subtract" * doctest::skip(false)) {
  auto value = minus(lit(10), lit(20))->eval();
      CHECK_EQ(value, -10);
}

TEST_CASE ("Multiply" * doctest::skip(false)) {
  auto value = times(lit(10), lit(20))->eval();
      CHECK_EQ(value, 200);
}

TEST_CASE ("Divide" * doctest::skip(false)) {
  auto value = divide(lit(10.0), lit(20.0))->eval();
      CHECK_EQ(value, 0.5);
}

TEST_CASE ("Composed" * doctest::skip(false)) {
  auto value = times(lit(10), plus(minus(lit(20), lit(5)), lit(5)))->eval();
      CHECK_EQ(value, 200);
}