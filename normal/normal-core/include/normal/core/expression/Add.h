//
// Created by matt on 2/4/20.
//

#ifndef NORMAL_NORMAL_CORE_SRC_EXPRESSION_PLUS_H
#define NORMAL_NORMAL_CORE_SRC_EXPRESSION_PLUS_H

#include "Expression.h"

#include <memory>

namespace normal::core::expression {

/**
 * FIXME: For some reason removing the namespace prefixes triggers on error in CLions editor.
 *
 * @tparam T
 */
template<typename T>
class Add : public normal::core::expression::Expression<T> {

private:
  std::unique_ptr<normal::core::expression::Expression<T>> left_;
  std::unique_ptr<normal::core::expression::Expression<T>> right_;

public:
  Add(std::unique_ptr<normal::core::expression::Expression<T>> left,
      std::unique_ptr<normal::core::expression::Expression<T>> right)
      : left_(std::move(left)), right_(std::move(right)) {
  }

};

template<typename T>
static std::unique_ptr<normal::core::expression::Expression<T>> plus(
    std::unique_ptr<normal::core::expression::Expression<T>> left,
    std::unique_ptr<normal::core::expression::Expression<T>> right) {
  return std::make_unique<Add<T>>(std::move(left), std::move(right));
}

}

#endif //NORMAL_NORMAL_CORE_SRC_EXPRESSION_PLUS_H
