//
// Created by matt on 2/4/20.
//

#include <memory>

#include <doctest/doctest.h>

#include <normal/core/expression/Expression.h>
#include <normal/core/expression/Literal.h>
#include <normal/core/expression/Add.h>
#include <normal/core/expression/ExpressionFactory.h>
#include "Globals.h"

TEST_CASE ("Literal" * doctest::skip(false)) {
  auto value = lit(10)->eval();
      CHECK_EQ(value, 10);
}

TEST_CASE ("Add" * doctest::skip(false)) {
  auto value = plus(lit(10), lit(20))->eval();
      CHECK_EQ(value, 30);
}

TEST_CASE ("Multiply" * doctest::skip(false)) {
  auto value = times(lit(10), lit(20))->eval();
      CHECK_EQ(value, 200);
}

TEST_CASE ("Composed" * doctest::skip(false)) {
  auto value = times(lit(10), plus(lit(15), lit(5)))->eval();
      CHECK_EQ(value, 200);
}