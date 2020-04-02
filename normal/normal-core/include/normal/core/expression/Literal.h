//
// Created by matt on 2/4/20.
//

#ifndef NORMAL_NORMAL_CORE_SRC_EXPRESSION_LITERAL_H
#define NORMAL_NORMAL_CORE_SRC_EXPRESSION_LITERAL_H

#include <memory>
#include "Expression.h"

namespace normal::core::expression {

template<class T>
class Literal : public Expression<T> {
private:
  T value_;

public:
  explicit Literal(T value) : value_(value) {}
  T eval() { return value_; }

};

template<class T>
static std::unique_ptr<Expression<T>> lit(T value){
  return std::make_unique<Literal<T>>(value);
}

}

#endif //NORMAL_NORMAL_CORE_SRC_EXPRESSION_LITERAL_H
