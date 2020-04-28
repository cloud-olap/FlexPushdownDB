//
// Created by matt on 27/4/20.
//

#ifndef NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_CAST_H
#define NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_CAST_H

#include <string>
#include <memory>

#include "Expression.h"

#include <normal/core/type/Type.h>

namespace normal::expression::gandiva {

class Cast : public Expression {

public:
  Cast(std::shared_ptr<Expression> value, std::shared_ptr<normal::core::type::Type> resultType);

  ::gandiva::NodePtr buildGandivaExpression(std::shared_ptr<arrow::Schema> Ptr) override;
  std::shared_ptr<arrow::DataType> resultType(std::shared_ptr<arrow::Schema> Ptr) override;
  void compile(std::shared_ptr<arrow::Schema> schema) override;
  std::string name() override;

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

#endif //NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_CAST_H
