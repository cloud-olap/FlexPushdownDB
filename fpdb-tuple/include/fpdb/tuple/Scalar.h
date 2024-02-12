//
// Created by matt on 1/5/20.
//

#ifndef FPDB_FPDB_TUPLE_INCLUDE_FPDB_TUPLE_SCALAR_H
#define FPDB_FPDB_TUPLE_INCLUDE_FPDB_TUPLE_SCALAR_H

#include <fpdb/tuple/serialization/ArrowSerializer.h>
#include <fpdb/caf/CAFUtil.h>
#include <arrow/scalar.h>
#include <tl/expected.hpp>
#include <memory>

namespace fpdb::tuple {

class Scalar {

public:
  explicit Scalar(std::shared_ptr<::arrow::Scalar> scalar);
  Scalar() = default;
  Scalar(const Scalar&) = default;
  Scalar& operator=(const Scalar&) = default;

  static std::shared_ptr<Scalar> make(const std::shared_ptr<::arrow::Scalar> &scalar);

  const std::shared_ptr<::arrow::Scalar> getArrowScalar() const;

  std::shared_ptr<::arrow::DataType> type();

  size_t hash();

  bool operator==(const Scalar &other) const;

  bool operator!=(const Scalar &other) const;

  std::string toString();

  template<typename T>
  tl::expected<T, std::string> value() {
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
    } else if (scalar_->type->id() == ::arrow::Date64Type::type_id) {
      auto typedScalar = std::static_pointer_cast<::arrow::Date64Scalar>(scalar_);
      auto typedValue = typedScalar->value;
      return typedValue;
    } else if (scalar_->type->id() == ::arrow::BooleanType::type_id) {
      auto typedScalar = std::static_pointer_cast<::arrow::BooleanScalar>(scalar_);
      auto typedValue = typedScalar->value;
      return typedValue;
    } else {
      return tl::make_unexpected("Scalar type '" + scalar_->type->ToString() + "' not implemented yet");
    }
  }

  template<>
  tl::expected<std::string, std::string> value<std::string>() {
    if (scalar_->type->id() == ::arrow::StringType::type_id) {
      auto typedScalar = std::static_pointer_cast<::arrow::StringScalar>(scalar_);
      auto typedValue = typedScalar->ToString();
      return typedValue;
    } else {
      return tl::make_unexpected("Scalar type '" + scalar_->type->ToString() + "' not implemented yet");
    }
  }

  template<>
  tl::expected<::arrow::Decimal128, std::string> value<::arrow::Decimal128>() {
    if (scalar_->type->id() == ::arrow::Decimal128Type::type_id) {
      auto typedScalar = std::static_pointer_cast<::arrow::Decimal128Scalar>(scalar_);
      auto typedValue = typedScalar->value;
      return typedValue;
    } else {
      return tl::make_unexpected("Scalar type '" + scalar_->type->ToString() + "' not implemented yet");
    }
  }

private:
  std::shared_ptr<::arrow::Scalar> scalar_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, Scalar& scalar) {
    auto toBytes = [&scalar]() -> decltype(auto) {
      return ArrowSerializer::scalar_to_bytes(scalar.scalar_);
    };
    auto fromBytes = [&scalar](const std::vector<std::uint8_t> &bytes) {
      scalar.scalar_ = ArrowSerializer::bytes_to_scalar(bytes);
      return true;
    };
    return f.object(scalar).fields(f.field("scalar", toBytes, fromBytes));
  }
};

}

using ScalarPtr = std::shared_ptr<fpdb::tuple::Scalar>;

CAF_BEGIN_TYPE_ID_BLOCK(Scalar, fpdb::caf::CAFUtil::Scalar_first_custom_type_id)
CAF_ADD_TYPE_ID(Scalar, (fpdb::tuple::Scalar))
CAF_END_TYPE_ID_BLOCK(Scalar)

namespace caf {
template <>
struct inspector_access<ScalarPtr> : variant_inspector_access<ScalarPtr> {
  // nop
};
} // namespace caf

#endif //FPDB_FPDB_TUPLE_INCLUDE_FPDB_TUPLE_SCALAR_H
