//
// Created by matt on 2/4/20.
//

#ifndef NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_EXPRESSION_DIVIDE_H
#define NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_EXPRESSION_DIVIDE_H

#include "Expression.h"

#include <memory>

namespace normal::core::expression {

template<class T>
class Divide : public normal::core::expression::Expression<T> {
private:
  std::unique_ptr<normal::core::expression::Expression<T>> left_;
  std::unique_ptr<normal::core::expression::Expression<T>> right_;

public:
  Divide(std::unique_ptr<normal::core::expression::Expression<T>> left,
         std::unique_ptr<normal::core::expression::Expression<T>> right)
      : left_(std::move(left)), right_(std::move(right)) {}

};

template<class T>
static std::unique_ptr<normal::core::expression::Expression<T>>
divide(std::unique_ptr<normal::core::expression::Expression<T>> left,
       std::unique_ptr<normal::core::expression::Expression<T>> right) {
  return std::make_unique<Divide<T>>(std::move(left), std::move(right));
}

}

#endif //NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_EXPRESSION_DIVIDE_H
