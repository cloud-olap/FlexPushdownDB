//
// Created by matt on 2/4/20.
//

#ifndef NORMAL_NORMAL_CORE_SRC_EXPRESSION_LITERAL_H
#define NORMAL_NORMAL_CORE_SRC_EXPRESSION_LITERAL_H

#include "Expression.h"

template<class T>
class Literal : public Expression<T> {
private:
  T value_;

public:
  explicit Literal(T value) : value_(value) {}
  T eval() { return value_; }

};

#endif //NORMAL_NORMAL_CORE_SRC_EXPRESSION_LITERAL_H
