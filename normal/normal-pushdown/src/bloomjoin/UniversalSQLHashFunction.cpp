//
// Created by matt on 7/8/20.
//

#include <fmt/format.h>

#include <normal/pushdown/bloomjoin/UniversalSQLHashFunction.h>

UniversalSQLHashFunction::UniversalSQLHashFunction(int M) : UniversalHashFunction(M) {}

std::shared_ptr<UniversalSQLHashFunction> UniversalSQLHashFunction::make(int m) {
  return std::make_shared<UniversalSQLHashFunction>(m);
}

std::string UniversalSQLHashFunction::sql(int x) {
  return fmt::format("(({} * cast({} as int) + {}) % {}) % {}", a_, x, b_, p_, m_);
}
