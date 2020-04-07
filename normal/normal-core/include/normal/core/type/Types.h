//
// Created by matt on 5/4/20.
//

#ifndef NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_TYPE_TYPES_H
#define NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_TYPE_TYPES_H

#include <memory>
#include <arrow/type.h>
#include "Type.h"
#include "DecimalType.h"
#include "Float64Type.h"

namespace normal::core::type {

class Types {
public:
  static std::unique_ptr<Type> fromArrowType(std::shared_ptr<arrow::DataType> arrowType) {
    if (arrowType->id() == arrow::DecimalType::type_id) {
      auto t = std::static_pointer_cast<arrow::DecimalType>(arrowType);
      return decimalType(t->precision(), t->scale());
    }
    else{
      abort();
    }
  }

  static std::unique_ptr<Type> fromStringType(std::string &&stringType) {
    if (stringType == "double") {
      return float64Type();
    }
    else{
      abort();
    }
  }

};

}

#endif //NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_TYPE_TYPES_H
