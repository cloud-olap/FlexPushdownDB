//
// Created by matt on 8/5/20.
//

#include "normal/core/type/Integer64Type.h"

using namespace normal::core::type;

std::shared_ptr<Type> normal::core::type::integer64Type() {
  return std::make_shared<Integer64Type>();
}
