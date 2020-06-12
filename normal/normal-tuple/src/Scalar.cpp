//
// Created by matt on 1/5/20.
//

#include "normal/tuple/Scalar.h"

#include <utility>

using namespace normal::tuple;

Scalar::Scalar(std::shared_ptr<::arrow::Scalar> scalar) : scalar_(std::move(scalar)) {}

std::shared_ptr<::arrow::DataType> Scalar::type() {
  return scalar_->type;
}

std::shared_ptr<Scalar> Scalar::make(const std::shared_ptr<::arrow::Scalar> &scalar) {
  return std::make_shared<Scalar>(scalar);
}

size_t Scalar::hash() {
  if (scalar_->type->id() == arrow::Int64Type::type_id) {
	auto typedScalar = std::static_pointer_cast<::arrow::Int64Scalar>(scalar_);
	return std::hash<long>()(typedScalar->value);
  }
  else if (scalar_->type->id() == arrow::Int32Type::type_id) {
	auto typedScalar = std::static_pointer_cast<::arrow::Int32Scalar>(scalar_);
	return std::hash<int>()(typedScalar->value);
  }
	else if (scalar_->type->id() == arrow::StringType::type_id) {
	  // FIXME: This is a bit of a hack, need to go to arrow 0.0.17 which properly implements hashes on scalars
	  auto typedScalar = std::static_pointer_cast<::arrow::StringScalar>(scalar_);
	  return std::hash<std::string>()(typedScalar->value->ToString());
  } else {
	throw std::runtime_error(
		"Hash on scalar type '" + scalar_->type->ToString() + "' not implemented yet");
  }
}

bool Scalar::operator==(const Scalar &other) const {
  return(scalar_->Equals(*other.scalar_));
}

bool Scalar::operator!=(const Scalar &other) const {
  return !(*this == other);
}

std::string Scalar::toString() {
  if(scalar_->type->id() == ::arrow::Decimal128Type::type_id){
	// Arrow cant convert decimal scalars to strings for some reason so we handle it as a special case
	auto decimalScalar = std::static_pointer_cast<::arrow::Decimal128Scalar>(scalar_);
	auto decimalType = std::static_pointer_cast<::arrow::Decimal128Type>(decimalScalar->type);
	return decimalScalar->value.ToString(decimalType->scale());
  }
  else{
	return scalar_->ToString();
  }
}
