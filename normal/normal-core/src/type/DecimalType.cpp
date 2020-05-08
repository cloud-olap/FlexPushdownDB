//
// Created by matt on 8/5/20.
//

#include "normal/core/type/DecimalType.h"

using namespace normal::core::type;

std::unique_ptr<Type> normal::core::type::decimalType(int precision, int scale) {
  return std::make_unique<DecimalType>(precision, scale);
}
