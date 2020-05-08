//
// Created by matt on 5/4/20.
//

#ifndef NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_TYPE_DECIMALTYPE_H
#define NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_TYPE_DECIMALTYPE_H

#include <arrow/type.h>

#include <normal/core/type/Type.h>

namespace normal::core::type {

class DecimalType: public Type {

public:
  explicit DecimalType(int precision, int scale) : Type("Decimal"), precision_(precision), scale_(scale) {}

  std::string asGandivaTypeString() override {
    return "DECIMAL";
  }

  std::shared_ptr<arrow::DataType> asArrowType() override {
    return arrow::decimal(precision_, scale_);
  }

private:
  int precision_;
  int scale_;

};

std::unique_ptr<Type> decimalType(int precision, int scale);

}

#endif //NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_TYPE_DECIMALTYPE_H
