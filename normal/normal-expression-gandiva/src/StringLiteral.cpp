//
// Created by Yifei Yang on 7/15/20.
//

#include "normal/expression/gandiva/StringLiteral.h"

using namespace normal::expression::gandiva;

StringLiteral::StringLiteral(std::string value) : value_(value) {}

void StringLiteral::compile(std::shared_ptr<arrow::Schema>){
  auto literal = ::gandiva::TreeExprBuilder::MakeStringLiteral(value_);

  gandivaExpression_ = literal;
  returnType_ = ::arrow::TypeTraits<::arrow::StringType>::type_singleton();
}

std::string StringLiteral::alias(){
  return "\'" + value_ + "\'";
}

std::shared_ptr<std::vector<std::string> > StringLiteral::involvedColumnNames() {
  return std::make_shared<std::vector<std::string>>();
}

const std::string &StringLiteral::value() const {
  return value_;
}

std::shared_ptr<Expression> normal::expression::gandiva::str_lit(std::string value){
  return std::make_shared<StringLiteral>(value);
}