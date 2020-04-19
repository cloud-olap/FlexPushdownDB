//
// Created by matt on 2/4/20.
//

#ifndef NORMAL_NORMAL_CORE_SRC_EXPRESSION_LITERAL_H
#define NORMAL_NORMAL_CORE_SRC_EXPRESSION_LITERAL_H

#include <memory>
#include <arrow/api.h>
#include "Expression.h"

namespace normal::expression {

template <typename T>
class Literal : public Expression {
private:
  T value_;

public:
  explicit Literal(T value) : value_(value) {}

};

template <typename T>
static std::shared_ptr<Expression> lit(T value){
  return std::make_shared<Literal<T>>(value);
}

}

#endif //NORMAL_NORMAL_CORE_SRC_EXPRESSION_LITERAL_H
