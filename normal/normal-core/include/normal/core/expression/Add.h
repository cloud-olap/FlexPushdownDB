//
// Created by matt on 2/4/20.
//

#ifndef NORMAL_NORMAL_CORE_SRC_EXPRESSION_PLUS_H
#define NORMAL_NORMAL_CORE_SRC_EXPRESSION_PLUS_H

#include "Expression.h"

#include <memory>

template<class T>
class Add : public Expression<T> {
private:
  std::unique_ptr<Expression<T>> left_;
  std::unique_ptr<Expression<T>> right_;

public:
  Add(std::unique_ptr<Expression<T>> left, std::unique_ptr<Expression<T>> right)
      : left_(std::move(left)), right_(std::move(right)) {}

  T eval() override {
    return left_->eval() + right_->eval();
  }

};

template<class T>
static std::unique_ptr<Expression<T>> plus(std::unique_ptr<Expression<T>> left, std::unique_ptr<Expression<T>> right){
  return std::make_unique<Add<T>>(std::move(left), std::move(right));
}

#endif //NORMAL_NORMAL_CORE_SRC_EXPRESSION_PLUS_H
