//
// Created by matt on 5/4/20.
//

#ifndef NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_EXPRESSION_TYPE_H
#define NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_EXPRESSION_TYPE_H

#include <memory>
#include <utility>

namespace normal::core::type {

class Type {
private:
  std::string name_;

public:
  explicit Type(std::string name) : name_(std::move(name)) {}
  virtual ~Type() = default;

  virtual std::shared_ptr<arrow::DataType> asArrowType() = 0;

  virtual std::string asGandivaTypeString() = 0;

  [[nodiscard]] const std::string &name() const {
    return name_;
  }

};

}

#endif //NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_EXPRESSION_TYPE_H
