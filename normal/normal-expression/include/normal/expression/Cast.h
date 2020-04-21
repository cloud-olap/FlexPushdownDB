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

namespace normal::expression {

/**
 * FIXME: For some reason removing the namespace prefixes triggers on error in CLions editor.
 *
 * @tparam T
 */
class Cast : public Expression {

public:
  Cast(std::shared_ptr<Expression> value,
	   std::shared_ptr<normal::core::type::Type> resultType);

  std::shared_ptr<arrow::DataType> resultType(std::shared_ptr<arrow::Schema>) override;

  void compile(std::shared_ptr<arrow::Schema> schema) override;
  std::string &name() override;

  gandiva::NodePtr buildGandivaExpression(std::shared_ptr<arrow::Schema> schema) override;

private:
  std::shared_ptr<Expression> value_;
  std::shared_ptr<normal::core::type::Type> resultType_;

};

static std::shared_ptr<Expression> cast(
	std::shared_ptr<Expression> value,
	std::shared_ptr<normal::core::type::Type> type) {
  return std::make_shared<Cast>(std::move(value), std::move(type));
}

}

#endif //NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_EXPRESSION_CAST_H
