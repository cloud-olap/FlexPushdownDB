//
// Created by matt on 2/4/20.
//

#ifndef NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_EXPRESSION_SUBTRACT_H
#define NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_EXPRESSION_SUBTRACT_H

#include "Expression.h"
#include <arrow/api.h>
#include <memory>

namespace normal::expression {

class Subtract : public normal::expression::Expression {
private:
  std::shared_ptr<normal::expression::Expression> left_;
  std::shared_ptr<normal::expression::Expression> right_;

public:
  Subtract(std::shared_ptr<normal::expression::Expression> left,
           std::shared_ptr<normal::expression::Expression> right) :
      left_(std::move(left)), right_(std::move(right)) {}

  gandiva::NodePtr buildGandivaExpression(std::shared_ptr<arrow::Schema> Ptr) override {
    return gandiva::NodePtr();
  }

  std::shared_ptr<arrow::DataType> resultType(std::shared_ptr<arrow::Schema> Ptr) override {
    return std::shared_ptr<arrow::DataType>();
  }
};

static std::shared_ptr<normal::expression::Expression>
minus(std::shared_ptr<normal::expression::Expression> left,
      std::shared_ptr<normal::expression::Expression> right) {
  return std::make_shared<Subtract>(std::move(left), std::move(right));
}

}

#endif //NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_EXPRESSION_SUBTRACT_H
