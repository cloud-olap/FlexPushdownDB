//
// Created by matt on 5/4/20.
//

#ifndef NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_TYPE_FLOAT64TYPE_H
#define NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_TYPE_FLOAT64TYPE_H


#include <utility>
#include <arrow/type.h>
#include "Type.h"

namespace normal::core::type {

class Float64Type: public Type {
private:

public:
  explicit Float64Type() : Type("Float64") {}

  std::string asGandivaTypeString() override {
    return "FLOAT8";
  }

  std::shared_ptr<arrow::DataType> asArrowType() override {
    return arrow::float64();
  }

};

static std::shared_ptr<Type> float64Type(){
  return std::make_shared<Float64Type>();
}

}


#endif //NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_TYPE_FLOAT64TYPE_H
