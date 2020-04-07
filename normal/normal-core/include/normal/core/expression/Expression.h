//
// Created by matt on 2/4/20.
//

#ifndef NORMAL_NORMAL_CORE_SRC_EXPRESSION_H
#define NORMAL_NORMAL_CORE_SRC_EXPRESSION_H

#include <utility>

#include <arrow/api.h>
#include <gandiva/gandiva_aliases.h>
#include <normal/core/type/Type.h>

namespace normal::core::expression {

class Expression {
private:
  std::string name_;

public:
  virtual ~Expression() = default;

  [[nodiscard]] const std::string &name() const {
    return name_;
  }

  virtual gandiva::NodePtr buildGandivaExpression(std::shared_ptr<arrow::Schema>) = 0;
  virtual std::shared_ptr<arrow::DataType> resultType(std::shared_ptr<arrow::Schema>) = 0;

};

}

#endif //NORMAL_NORMAL_CORE_SRC_EXPRESSION_H
