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
#include <normal/core/type/Float64Type.h>

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

  std::string &name() override {
    return value_->name();
  }

  gandiva::NodePtr buildGandivaExpression(std::shared_ptr<arrow::Schema> schema) override {

    auto paramGandivaExpression = value_->buildGandivaExpression(schema);
    auto fromArrowType = value_->resultType(schema);
    auto toArrowType = resultType_->asArrowType();

    /**
     * NOTE: Some cast operations are not supported by Gandiva so we set up some special cases here
     */
    if (fromArrowType->id() == arrow::utf8()->id() && toArrowType->id() == arrow::float64()->id()) {
      // Not supported directly by Gandiva, need to cast string to decimal and then that to float64

      auto castDecimalFunctionName = "castDECIMAL";
      auto castDecimalReturnType = arrow::decimal(10, 5); // FIXME: Need to check if this is sufficient to cast to double
      auto castToDecimalFunction = gandiva::TreeExprBuilder::MakeFunction(castDecimalFunctionName,
                                                                          {paramGandivaExpression},
                                                                          castDecimalReturnType);

      auto castFunctionName = "cast" + resultType_->asGandivaTypeString();
      auto castReturnType = resultType_->asArrowType();

      auto castFunction = gandiva::TreeExprBuilder::MakeFunction(castFunctionName,
                                                                 {castToDecimalFunction},
                                                                 castReturnType);

      return castFunction;

    } else {

      auto function = "cast" + resultType_->asGandivaTypeString();
      auto returnType = resultType_->asArrowType();

      return gandiva::TreeExprBuilder::MakeFunction(function, {paramGandivaExpression}, returnType);
    }
  }

};

static std::shared_ptr<normal::core::expression::Expression> cast(
    std::shared_ptr<normal::core::expression::Expression> value,
    std::shared_ptr<normal::core::type::Type> type) {
  return std::make_shared<Cast>(std::move(value), std::move(type));
}

}

#endif //NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_EXPRESSION_CAST_H
