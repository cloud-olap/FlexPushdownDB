//
// Created by matt on 2/4/20.
//

#ifndef NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_EXPRESSION_EXPRESSIONFACTORY_H
#define NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_EXPRESSION_EXPRESSIONFACTORY_H

#include <memory>

#include "Literal.h"
#include "Add.h"
#include "Multiply.h"

template<class T>
static std::unique_ptr<Expression<T>> lit(T value){
  return std::make_unique<Literal<T>>(value);
}

template<class T>
static std::unique_ptr<Expression<T>> plus(std::unique_ptr<Expression<T>> left, std::unique_ptr<Expression<T>> right){
  return std::make_unique<Add<T>>(std::move(left), std::move(right));
}

template<class T>
static std::unique_ptr<Expression<T>> times(std::unique_ptr<Expression<T>> left, std::unique_ptr<Expression<T>> right){
  return std::make_unique<Multiply<T>>(std::move(left), std::move(right));
}


#endif //NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_EXPRESSION_EXPRESSIONFACTORY_H
