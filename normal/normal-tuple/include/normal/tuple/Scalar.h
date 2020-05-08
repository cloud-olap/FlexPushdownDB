//
// Created by matt on 1/5/20.
//

#ifndef NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_SCALAR_H
#define NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_SCALAR_H

#include <memory>

#include <arrow/visitor_inline.h>
#include <arrow/scalar.h>

namespace normal::tuple {

class Scalar {

public:

  explicit Scalar(std::shared_ptr<::arrow::Scalar> scalar);

  static std::shared_ptr<Scalar> make(const std::shared_ptr<::arrow::Scalar> &scalar) {
	if (scalar->type->id() == arrow::int64()->id()) {
	  return std::make_shared<Scalar>(scalar);
	} else {
	  throw std::runtime_error(
		  "Column type '" + scalar->type->ToString() + "' not implemented yet");
	}
  }

  std::shared_ptr<::arrow::DataType> type();

  size_t hash() {
	if (scalar_->type->id() == arrow::int64()->id()) {
	  auto typedScalar = std::static_pointer_cast<::arrow::Int64Scalar>(scalar_);
	  return std::hash<long>()(typedScalar->value);
	} else {
	  throw std::runtime_error(
		  "Scalar type '" + scalar_->type->ToString() + "' not implemented yet");
	}
  };

  bool operator==(const Scalar &other) const {
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

  std::string toString() {
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

  template<typename T>
  T value() {
	if (scalar_->type->id() == ::arrow::Int32Type::type_id) {
	  auto typedScalar = std::static_pointer_cast<::arrow::Int32Scalar>(scalar_);
	  auto typedValue = typedScalar->value;
	  return typedValue;
	} else if (scalar_->type->id() == ::arrow::Int64Type::type_id) {
	  auto typedScalar = std::static_pointer_cast<::arrow::Int64Scalar>(scalar_);
	  auto typedValue = typedScalar->value;
	  return typedValue;
	} else if (scalar_->type->id() == ::arrow::FloatType::type_id) {
	  auto typedScalar = std::static_pointer_cast<::arrow::FloatScalar>(scalar_);
	  auto typedValue = typedScalar->value;
	  return typedValue;
	} else if (scalar_->type->id() == ::arrow::DoubleType::type_id) {
	  auto typedScalar = std::static_pointer_cast<::arrow::DoubleScalar>(scalar_);
	  auto typedValue = typedScalar->value;
	  return typedValue;
	} else if (scalar_->type->id() == ::arrow::BooleanType::type_id) {
	  auto typedScalar = std::static_pointer_cast<::arrow::BooleanScalar>(scalar_);
	  auto typedValue = typedScalar->value;
	  return typedValue;
	} else {
	  throw std::runtime_error(
		  "Scalar type '" + scalar_->type->ToString() + "' not implemented yet");
	}
  }

  template<>
  ::arrow::Decimal128 value<::arrow::Decimal128>() {
	if (scalar_->type->id() == ::arrow::Decimal128Type::type_id) {
	  auto typedScalar = std::static_pointer_cast<::arrow::Decimal128Scalar>(scalar_);
	  auto typedValue = typedScalar->value;
	  return typedValue;
	} else {
	  throw std::runtime_error(
		  "Scalar type '" + scalar_->type->ToString() + "' not implemented yet");
	}
  }

  std::string showString();

private:
  std::shared_ptr<::arrow::Scalar> scalar_;

};

}

#endif //NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_SCALAR_H
