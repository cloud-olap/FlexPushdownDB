//
// Created by matt on 15/4/20.
//

#ifndef NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_TYPE_INTEGER32TYPE_H
#define NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_TYPE_INTEGER32TYPE_H

#include <arrow/api.h>
#include <normal/core/type/Type.h>

namespace normal::core::type {

class Integer32Type: public Type {
private:

public:
  explicit Integer32Type() : Type("Int32") {}

  std::string asGandivaTypeString() override {
	return "INT";
  }

  std::shared_ptr<arrow::DataType> asArrowType() override {
	return arrow::int32();
  }

};

static std::shared_ptr<Type> integer32Type(){
  return std::make_shared<Integer32Type>();
}

}

#endif //NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_TYPE_INTEGER32TYPE_H
