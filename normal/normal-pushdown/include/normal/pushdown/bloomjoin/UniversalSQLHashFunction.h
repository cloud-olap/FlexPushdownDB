//
// Created by matt on 5/8/20.
//

#ifndef NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_BLOOMJOIN_UNIVERSALSQLHASHFUNCTION_H
#define NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_BLOOMJOIN_UNIVERSALSQLHASHFUNCTION_H

#include <memory>

class UniversalSQLHashFunction {

public:
  explicit UniversalSQLHashFunction(int numBits);
  [[nodiscard]] static std::shared_ptr<UniversalSQLHashFunction> make(int numBits);

  [[nodiscard]] size_t hash(int k) const;
  [[nodiscard]] std::string sql(int x, int m);

private:
  int m_;
  int p_;
  int a_;
  int b_;

};

#endif //NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_BLOOMJOIN_UNIVERSALSQLHASHFUNCTION_H
