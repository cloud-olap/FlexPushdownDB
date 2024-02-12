//
// Created by matt on 1/5/20.
//

#include "fpdb/tuple/Scalar.h"

#include <utility>

using namespace fpdb::tuple;

Scalar::Scalar(std::shared_ptr<::arrow::Scalar> scalar) : scalar_(std::move(scalar)) {}

std::shared_ptr<Scalar> Scalar::make(const std::shared_ptr<::arrow::Scalar> &scalar) {
  return std::make_shared<Scalar>(scalar);
}

std::shared_ptr<::arrow::DataType> Scalar::type() {
  return scalar_->type;
}

const std::shared_ptr<::arrow::Scalar> Scalar::getArrowScalar() const {
  return scalar_;
}

size_t Scalar::hash() {
  return scalar_->hash();
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
