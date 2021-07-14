//
// Created by matt on 8/5/20.
//

#include "normal/core/type/Integer32Type.h"

using namespace normal::core::type;

std::shared_ptr<Type> normal::core::type::integer32Type() {
  return std::make_shared<Integer32Type>();
}
