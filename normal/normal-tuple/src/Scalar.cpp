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
  if (scalar->type->id() == arrow::int64()->id()) {
	return std::make_shared<Scalar>(scalar);
  } else {
	throw std::runtime_error(
		"Column type '" + scalar->type->ToString() + "' not implemented yet");
  }
}

size_t Scalar::hash() {
  if (scalar_->type->id() == arrow::int64()->id()) {
	auto typedScalar = std::static_pointer_cast<::arrow::Int64Scalar>(scalar_);
	return std::hash<long>()(typedScalar->value);
  } else {
	throw std::runtime_error(
		"Scalar type '" + scalar_->type->ToString() + "' not implemented yet");
  }
}

bool Scalar::operator==(const Scalar &other) const {
  if (scalar_->type->id() == other.scalar_->type->id()) {
	if (scalar_->type->id() == arrow::int64()->id()) {
	  auto typedScalar = std::static_pointer_cast<::arrow::Int64Scalar>(scalar_);
	  auto typedOther = std::static_pointer_cast<::arrow::Int64Scalar>(other.scalar_);
	  return typedScalar->value == typedOther->value;
	} else {
	  throw std::runtime_error(
		  "Scalar type '" + scalar_->type->ToString() + "' not implemented yet");
	}
  } else {
	return false;
  }
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
