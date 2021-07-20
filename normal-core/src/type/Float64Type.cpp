//
// Created by matt on 8/5/20.
//

#include "normal/core/type/Float64Type.h"

using namespace normal::core::type;

std::shared_ptr<Type> normal::core::type::float64Type() {
  return std::make_shared<Float64Type>();
}
