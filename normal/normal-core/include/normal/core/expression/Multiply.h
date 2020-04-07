//
// Created by matt on 2/4/20.
//

#ifndef NORMAL_NORMAL_CORE_SRC_EXPRESSION_MULTIPLY_H
#define NORMAL_NORMAL_CORE_SRC_EXPRESSION_MULTIPLY_H

#include "Expression.h"
#include <arrow/api.h>
#include <memory>

namespace normal::core::expression {

class Multiply : public normal::core::expression::Expression {
private:
  std::shared_ptr<normal::core::expression::Expression> left_;
  std::shared_ptr<normal::core::expression::Expression> right_;

public:
  Multiply(std::shared_ptr<normal::core::expression::Expression> left,
           std::shared_ptr<normal::core::expression::Expression> right)
      : left_(std::move(left)), right_(std::move(right)) {}

  gandiva::NodePtr buildGandivaExpression(std::shared_ptr<arrow::Schema> Ptr) override {
    return gandiva::NodePtr();
  }

  std::shared_ptr<arrow::DataType> resultType(std::shared_ptr<arrow::Schema> Ptr) override {
    return std::shared_ptr<arrow::DataType>();
  }
};

static std::shared_ptr<normal::core::expression::Expression>
times(std::shared_ptr<normal::core::expression::Expression> left,
      std::shared_ptr<normal::core::expression::Expression> right) {
  return std::make_shared<Multiply>(std::move(left), std::move(right));
}

}

#endif //NORMAL_NORMAL_CORE_SRC_EXPRESSION_MULTIPLY_H
