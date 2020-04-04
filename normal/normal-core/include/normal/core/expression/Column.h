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

class Column : public Expression {
private:
  std::string name_;

public:
  explicit Column(std::string name) : name_(std::move(name)) {}

  [[nodiscard]] const std::string &name() const override {
    return name_;
  }

  gandiva::NodePtr buildGandivaExpression(std::shared_ptr<arrow::Schema> schema) override {
    return gandiva::TreeExprBuilder::MakeField(schema->GetFieldByName(name_));
  }

  std::shared_ptr<arrow::DataType> resultType(std::shared_ptr<arrow::Schema> schema) override {
    return schema->GetFieldByName(name_)->type();
  }
};

static std::unique_ptr<Expression> col(std::string name){
  return std::make_unique<Column>(std::move(name));
}

}


#endif //NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_EXPRESSION_COLUMN_H
