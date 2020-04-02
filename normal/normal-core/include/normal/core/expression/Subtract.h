//
// Created by matt on 2/4/20.
//

#ifndef NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_EXPRESSION_SUBTRACT_H
#define NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_EXPRESSION_SUBTRACT_H

#include "Expression.h"

#include <memory>

namespace normal::core::expression {

template<class T>
class Subtract : public normal::core::expression::Expression<T> {
private:
  std::unique_ptr<normal::core::expression::Expression<T>> left_;
  std::unique_ptr<normal::core::expression::Expression<T>> right_;

public:
  Subtract(std::unique_ptr<normal::core::expression::Expression<T>> left,
           std::unique_ptr<normal::core::expression::Expression<T>> right) :
      left_(std::move(left)), right_(std::move(right)) {}

  T eval() override {
    return left_->eval() - right_->eval();
  }

};

template<class T>
static std::unique_ptr<normal::core::expression::Expression<T>>
minus(std::unique_ptr<normal::core::expression::Expression<T>> left,
      std::unique_ptr<normal::core::expression::Expression<T>> right) {
  return std::make_unique<Subtract<T>>(std::move(left), std::move(right));
}

}

#endif //NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_EXPRESSION_SUBTRACT_H
