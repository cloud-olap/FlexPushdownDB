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
#include "Integer32Type.h"

namespace normal::core::type {

class [[maybe_unused]] Types {
public:
  [[maybe_unused]] static std::unique_ptr<Type> fromArrowType(const std::shared_ptr<arrow::DataType> &arrowType) {
	if (arrowType->id() == arrow::DecimalType::type_id) {
	  auto t = std::static_pointer_cast<arrow::DecimalType>(arrowType);
	  return decimalType(t->precision(), t->scale());
	} else {
	  throw std::runtime_error("Unrecognized type " + arrowType->ToString());
	}
  }

  [[maybe_unused]] static std::shared_ptr<Type> fromStringType(std::string &&stringType) {

	// Make sure its in lower case
	std::transform(stringType.begin(), stringType.end(), stringType.begin(), ::tolower);

	if (stringType == "double" || stringType == "float64") {
	  return float64Type();
	}
	if (stringType == "int" || stringType == "integer" || stringType == "int32" || stringType == "integer32") {
	  return integer32Type();
	} else {
	  throw std::runtime_error("Unrecognized type " + stringType);
	}
  }

};

}

#endif //NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_TYPE_TYPES_H
