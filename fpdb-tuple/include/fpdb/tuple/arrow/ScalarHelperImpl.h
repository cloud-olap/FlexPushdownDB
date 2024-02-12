//
// Created by matt on 10/4/20.
//

#ifndef NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_ARROW_SCALARHELPERIMPL_H
#define NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_ARROW_SCALARHELPERIMPL_H


#include <tl/expected.hpp>
#include "arrow/api.h"
#include "arrow/scalar.h"
#include "normal/tuple/arrow/ScalarHelper.h"

template<typename ARROW_TYPE, typename C_TYPE = typename ARROW_TYPE::c_type>
class ScalarHelperImpl : public ScalarHelper {

  using ARROW_SCALAR_TYPE = typename arrow::TypeTraits<ARROW_TYPE>::ScalarType;

private:
  C_TYPE value_;

public:

  explicit ScalarHelperImpl(std::shared_ptr<ARROW_SCALAR_TYPE> Value) : value_(Value->value) {}

  std::shared_ptr<arrow::Scalar> asScalar() override {
    return arrow::MakeScalar(value_);
  }

  ScalarHelper &operator+=(const std::shared_ptr<ScalarHelper> &rhs) override {
    auto typedRHS = std::static_pointer_cast<ScalarHelperImpl<ARROW_TYPE, C_TYPE>>(rhs);
    this->value_ += typedRHS->value_;
    return *this;
  }

  std::string toString() override {
    return std::to_string(this->value_);
  }
};

#endif //NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_ARROW_SCALARHELPERIMPL_H
