//
// Created by matt on 2/4/20.
//

#ifndef NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_EXPRESSION_COLUMN_H
#define NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_EXPRESSION_COLUMN_H


#include <memory>
#include <utility>
#include <normal/core/TupleSet.h>

#include "Expression.h"

namespace normal::core::expression {

template<typename T>
class Column : public Expression<T> {
private:
  std::string name_;

public:
  explicit Column(std::string name) : name_(std::move(name)) {}
  T eval() { return 0; }

};

template<typename T>
static std::unique_ptr<Expression<T>> col(std::string name){
  return std::make_unique<Column<T>>(name);
}

}


#endif //NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_EXPRESSION_COLUMN_H
