//
// Created by matt on 27/4/20.
//

#ifndef NORMAL_NORMAL_EXPRESSION_SIMPLE_INCLUDE_NORMAL_EXPRESSION_SIMPLE_CAST_H
#define NORMAL_NORMAL_EXPRESSION_SIMPLE_INCLUDE_NORMAL_EXPRESSION_SIMPLE_CAST_H

#include <memory>

#include <normal/core/type/Type.h>

#include "Expression.h"

namespace normal::expression::simple {

class Cast : public Expression {
public:
  Cast(std::shared_ptr<Expression> value, std::shared_ptr<normal::core::type::Type> resultType);

  std::string &alias() override;
  std::shared_ptr<arrow::DataType> resultType(std::shared_ptr<arrow::Schema>) override;
  tl::expected<std::shared_ptr<arrow::Array>, std::string> evaluate(const arrow::RecordBatch &recordBatch) override;
  void compile(std::shared_ptr<arrow::Schema> schema) override;

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

#endif //NORMAL_NORMAL_EXPRESSION_SIMPLE_INCLUDE_NORMAL_EXPRESSION_SIMPLE_CAST_H
