//
// Created by matt on 7/8/20.
//

#ifndef NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_BLOOMJOIN_UNIVERSALSQLHASHFUNCTION_H
#define NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_BLOOMJOIN_UNIVERSALSQLHASHFUNCTION_H

#include <memory>

#include "UniversalHashFunction.h"

/**
 * Additionally defines a sql method for generating SQL predicates from the hash function
 */
class UniversalSQLHashFunction : public UniversalHashFunction {

public:
  explicit UniversalSQLHashFunction(int m);
  [[nodiscard]] static std::shared_ptr<UniversalSQLHashFunction> make(int m);

  [[nodiscard]] std::string sql(int x);

};

#endif //NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_BLOOMJOIN_UNIVERSALSQLHASHFUNCTION_H
