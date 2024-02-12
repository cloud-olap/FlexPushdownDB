//
// Created by Yifei Yang on 12/11/21.
//

#include <fpdb/expression/gandiva/Like.h>
#include <gandiva/tree_expr_builder.h>

namespace fpdb::expression::gandiva {

Like::Like(const shared_ptr<Expression> &left, const shared_ptr<Expression> &right):
  BinaryExpression(left, right, LIKE) {}

void Like::compile(const shared_ptr<arrow::Schema> &schema) {
  left_->compile(schema);
  right_->compile(schema);

  returnType_ = arrow::boolean();
  gandivaExpression_ = ::gandiva::TreeExprBuilder::MakeFunction(
          "like",
          {left_->getGandivaExpression(), right_->getGandivaExpression()},
          returnType_);
}

std::string Like::alias() {
  return left_->alias() + " like " + right_->alias();
}

std::string Like::getTypeString() const {
  return "Like";
}

tl::expected<std::shared_ptr<Like>, std::string> Like::fromJson(const nlohmann::json &jObj) {
  auto expOperands = BinaryExpression::fromJson(jObj);
  if (!expOperands.has_value()) {
    return tl::make_unexpected(expOperands.error());
  }
  return std::make_shared<Like>((*expOperands).first, (*expOperands).second);
}

shared_ptr<Expression> like(const shared_ptr<Expression>& left, const shared_ptr<Expression>& right) {
  return make_shared<Like>(left, right);
}

}
