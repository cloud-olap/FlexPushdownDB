//
// Created by matt on 27/4/20.
//

#include <tl/expected.hpp>
#include "normal/expression/simple/Cast.h"

using namespace normal::expression::simple;

Cast::Cast(std::shared_ptr<Expression> expr, std::shared_ptr<normal::core::type::Type> type) :
	expr_(std::move(expr)),
	type_(std::move(type)) {
}

std::string Cast::alias() {
  return expr_->alias();
}

tl::expected<std::shared_ptr<arrow::Array>, std::string> Cast::evaluate(const arrow::RecordBatch &batch) {

  std::shared_ptr<arrow::Array> returnArray;

  auto evaluatedValue = expr_->evaluate(batch);
  if (evaluatedValue.value()->type()->id() == arrow::utf8()->id()) {
	auto typedEvaluatedValue = std::static_pointer_cast<arrow::StringArray>(evaluatedValue.value());

	if (type_->asArrowType()->id() == arrow::DoubleType::type_id) {
	  auto b = arrow::DoubleBuilder(arrow::default_memory_pool());
	  for (int r = 0; r < typedEvaluatedValue->length(); ++r) {
		auto s = typedEvaluatedValue->GetString(r);
		auto castValue = std::stod(s);
		auto res = b.Append(castValue);
		res = b.Finish(&returnArray);
	  }
	} else {
	  return tl::unexpected("Cast to " + type_->asArrowType()->ToString() + " not implemented yet");
	}
  } else {
	return tl::unexpected("Cast from " + evaluatedValue.value()->type()->ToString() + " not implemented yet");
  }

  return returnArray;
}

void Cast::compile(std::shared_ptr<arrow::Schema>) {
  // NOOP
}

std::shared_ptr<Expression> normal::expression::simple::cast(std::shared_ptr<Expression> expr,
															 std::shared_ptr<normal::core::type::Type> type) {
  return std::make_shared<Cast>(std::move(expr), std::move(type));
}
