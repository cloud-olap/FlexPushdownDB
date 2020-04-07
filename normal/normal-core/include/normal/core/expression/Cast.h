//
// Created by matt on 5/4/20.
//

#ifndef NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_EXPRESSION_CAST_H
#define NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_EXPRESSION_CAST_H

#include "Expression.h"

#include <arrow/api.h>
#include "normal/core/type/Type.h"

#include <memory>
#include <gandiva/tree_expr_builder.h>

namespace normal::core::expression {

/**
 * FIXME: For some reason removing the namespace prefixes triggers on error in CLions editor.
 *
 * @tparam T
 */
class Cast : public normal::core::expression::Expression {

private:
  std::shared_ptr<normal::core::expression::Expression> value_;
  std::shared_ptr<normal::core::type::Type> resultType_;

public:
  Cast(std::shared_ptr<normal::core::expression::Expression> value,
      std::shared_ptr<normal::core::type::Type> resultType)
      : value_(std::move(value)), resultType_(std::move(resultType)) {
  }

  std::shared_ptr<arrow::DataType> resultType(std::shared_ptr<arrow::Schema>) override {
    return resultType_->asArrowType();
  }

  gandiva::NodePtr buildGandivaExpression(std::shared_ptr<arrow::Schema> schema) override {

    auto function = "cast" + resultType_->asGandivaTypeString();
    auto param1 = value_->buildGandivaExpression(schema);
    auto returnType = resultType_->asArrowType();

    return gandiva::TreeExprBuilder::MakeFunction(function, {param1}, returnType);
  }

};

static std::shared_ptr<normal::core::expression::Expression> cast(
    std::shared_ptr<normal::core::expression::Expression> value,
    std::shared_ptr<normal::core::type::Type> type) {
  return std::make_shared<Cast>(std::move(value), std::move(type));
}

}

#endif //NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_EXPRESSION_CAST_H
