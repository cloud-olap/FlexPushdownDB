//
// Created by matt on 27/4/20.
//

#include "normal/expression/gandiva/Cast.h"

#include <utility>

#include <gandiva/tree_expr_builder.h>

using namespace normal::expression::gandiva;

Cast::Cast(std::shared_ptr<Expression> expr, std::shared_ptr<normal::core::type::Type> type) :
	expr_(std::move(expr)), type_(std::move(type)) {
}

::gandiva::NodePtr Cast::buildGandivaExpression() {

  auto paramGandivaExpression = expr_->getGandivaExpression();
  auto fromArrowType = expr_->getReturnType();
  auto toArrowType = type_->asArrowType();

  /**
   * NOTE: Some cast operations are not supported by Gandiva so we set up some special cases here
   */
  if (fromArrowType->id() == arrow::utf8()->id() && toArrowType->id() == arrow::float64()->id()) {
	// Not supported directly by Gandiva, need to cast string to decimal and then that to float64

	auto castDecimalFunctionName = "castDECIMAL";
	auto castDecimalReturnType = arrow::decimal(36, 2); // FIXME: Need to check if this is sufficient to cast to double
	auto castToDecimalFunction = ::gandiva::TreeExprBuilder::MakeFunction(castDecimalFunctionName,
																		  {paramGandivaExpression},
																		  castDecimalReturnType);

	auto castFunctionName = "cast" + type_->asGandivaTypeString();
	auto castReturnType = type_->asArrowType();

	auto castFunction = ::gandiva::TreeExprBuilder::MakeFunction(castFunctionName,
																 {castToDecimalFunction},
																 castReturnType);

	return castFunction;
  } else if (fromArrowType->id() == arrow::utf8()->id() && toArrowType->id() == arrow::int64()->id()) {
	// Not supported directly by Gandiva, need to cast string to decimal and then that to int64

	auto castDecimalFunctionName = "castDECIMAL";
	auto castDecimalReturnType = arrow::decimal(38, 0); // FIXME: Need to check if this is sufficient to cast to double
	auto castToDecimalFunction = ::gandiva::TreeExprBuilder::MakeFunction(castDecimalFunctionName,
																		  {paramGandivaExpression},
																		  castDecimalReturnType);

	auto castFunctionName = "cast" + type_->asGandivaTypeString();
	auto castReturnType = type_->asArrowType();

	auto castFunction = ::gandiva::TreeExprBuilder::MakeFunction(castFunctionName,
																 {castToDecimalFunction},
																 castReturnType);

	return castFunction;
  }
  else if (fromArrowType->id() == arrow::utf8()->id() && toArrowType->id() == arrow::int32()->id()) {
	// Not supported directly by Gandiva, need to cast string to decimal to int64 and then that to int32

	auto castDecimalFunctionName = "castDECIMAL";
	auto castDecimalReturnType = arrow::decimal(38, 0); // FIXME: Need to check if this is sufficient to cast to double
	auto castToDecimalFunction = ::gandiva::TreeExprBuilder::MakeFunction(castDecimalFunctionName,
																		  {paramGandivaExpression},
																		  castDecimalReturnType);

	auto castToInt64Function = ::gandiva::TreeExprBuilder::MakeFunction("castBIGINT",
																		  {castToDecimalFunction},
																		  ::arrow::int64());

	auto castFunctionName = "cast" + type_->asGandivaTypeString();
	auto castReturnType = type_->asArrowType();

	auto castFunction = ::gandiva::TreeExprBuilder::MakeFunction(castFunctionName,
																 {castToInt64Function},
																 castReturnType);

	return castFunction;
  } else {

	auto function = "cast" + type_->asGandivaTypeString();
	auto returnType = type_->asArrowType();

	auto expressionNode = ::gandiva::TreeExprBuilder::MakeFunction(function, {paramGandivaExpression}, returnType);

	return expressionNode;
  }
}

void Cast::compile(std::shared_ptr<arrow::Schema> schema) {
  expr_->compile(schema);

  gandivaExpression_ = buildGandivaExpression();
  returnType_ = type_->asArrowType();
}

std::string Cast::alias() {
  return expr_->alias();
}

std::shared_ptr<std::vector<std::string> > Cast::involvedColumnNames() {
  return expr_->involvedColumnNames();
}

std::shared_ptr<Expression> normal::expression::gandiva::cast(std::shared_ptr<Expression> expr,
															  std::shared_ptr<normal::core::type::Type> type) {
  return std::make_shared<Cast>(std::move(expr), std::move(type));
}
