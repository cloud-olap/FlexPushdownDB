//
// Created by matt on 27/4/20.
//

#include <tl/expected.hpp>
#include "normal/expression/simple/Cast.h"

using namespace normal::expression::simple;

Cast::Cast(std::shared_ptr<Expression> value, std::shared_ptr<normal::core::type::Type> resultType) :
	value_(std::move(value)),
	resultType_(std::move(resultType)) {
}

std::string &Cast::alias() {
  return value_->alias();
}

std::shared_ptr<arrow::DataType> Cast::resultType(std::shared_ptr<arrow::Schema>) {
  return resultType_->asArrowType();
}

tl::expected<std::shared_ptr<arrow::Array>, std::string> Cast::evaluate(const arrow::RecordBatch &recordBatch) {

  std::shared_ptr<arrow::Array> returnArray;

  auto evaluatedValue = value_->evaluate(recordBatch);
  if(evaluatedValue.value()->type()->id() == arrow::utf8()->id()){
    auto typedEvaluatedValue = std::static_pointer_cast<arrow::StringArray>(evaluatedValue.value());

	if(resultType_->asArrowType()->id() == arrow::DoubleType::type_id){
	  auto b = arrow::DoubleBuilder(arrow::default_memory_pool());
	  for (int r = 0; r <typedEvaluatedValue->length(); ++r) {
		auto s = typedEvaluatedValue->GetString(r);
		auto castValue = std::stod(s);
		auto res = b.Append(castValue);
		res = b.Finish(&returnArray);
	  }
	}
	else{
	  return tl::unexpected("Cast to " + resultType_->asArrowType()->ToString() + " not implemented yet");
	}
  }
  else{
    return tl::unexpected("Cast from " + evaluatedValue.value()->type()->ToString() + " not implemented yet");
  }

  return returnArray;
}

void Cast::compile(std::shared_ptr<arrow::Schema>) {
	// NOOP
}
