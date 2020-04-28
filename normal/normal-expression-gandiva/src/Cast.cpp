//
// Created by matt on 27/4/20.
//

#include "normal/expression/gandiva/Cast.h"

#include <gandiva/tree_expr_builder.h>

using namespace normal::expression::gandiva;

Cast::Cast(std::shared_ptr<Expression> value, std::shared_ptr<normal::core::type::Type> resultType) :
	value_(std::move(value)),
	resultType_(std::move(resultType)) {
}

::gandiva::NodePtr Cast::buildGandivaExpression(std::shared_ptr<arrow::Schema> schema) {

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
	auto castToDecimalFunction = ::gandiva::TreeExprBuilder::MakeFunction(castDecimalFunctionName,
																		{paramGandivaExpression},
																		castDecimalReturnType);

	auto castFunctionName = "cast" + resultType_->asGandivaTypeString();
	auto castReturnType = resultType_->asArrowType();

	auto castFunction = ::gandiva::TreeExprBuilder::MakeFunction(castFunctionName,
															   {castToDecimalFunction},
															   castReturnType);

	return castFunction;

  } else {

	auto function = "cast" + resultType_->asGandivaTypeString();
	auto returnType = resultType_->asArrowType();

	auto expressionNode = ::gandiva::TreeExprBuilder::MakeFunction(function, {paramGandivaExpression}, returnType);

	return expressionNode;
  }
}

std::shared_ptr<arrow::DataType> Cast::resultType(std::shared_ptr<arrow::Schema>) {
  return resultType_->asArrowType();
}

void Cast::compile(std::shared_ptr<arrow::Schema> schema) {
  gandivaExpression_ = buildGandivaExpression(schema);
  returnType_ = resultType(schema);
}

std::string Cast::name() {
  return value_->name();
}
