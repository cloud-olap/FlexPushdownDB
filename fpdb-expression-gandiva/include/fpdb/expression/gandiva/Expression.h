//
// Created by matt on 27/4/20.
//

#ifndef FPDB_FPDB_EXPRESSION_GANDIVA_INCLUDE_FPDB_EXPRESSION_GANDIVA_EXPRESSION_H
#define FPDB_FPDB_EXPRESSION_GANDIVA_INCLUDE_FPDB_EXPRESSION_GANDIVA_EXPRESSION_H

#include <fpdb/expression/Expression.h>
#include <fpdb/expression/gandiva/ExpressionType.h>
#include <arrow/type.h>
#include <gandiva/node.h>
#include <nlohmann/json.hpp>
#include <tl/expected.hpp>
#include <memory>
#include <set>

namespace fpdb::expression::gandiva {

class Expression : public fpdb::expression::Expression {

public:
  Expression(ExpressionType type);
  Expression() = default;
  Expression(const Expression&) = default;
  Expression& operator=(const Expression&) = default;

  virtual ~Expression() = default;

  virtual std::set<std::string> involvedColumnNames() = 0;

  virtual std::string getTypeString() const = 0;

  virtual ::nlohmann::json toJson() const = 0;
  static tl::expected<std::shared_ptr<fpdb::expression::gandiva::Expression>, std::string>
  fromJson(const nlohmann::json &jObj);

  ExpressionType getType() const;
  const ::gandiva::NodePtr &getGandivaExpression() const;

  std::string showString();

protected:
  ExpressionType type_;
  ::gandiva::NodePtr gandivaExpression_;

};

const inline std::string prefixInt_ = "int:";
const inline std::string prefixFloat_ = "float:";
std::shared_ptr<std::string> removePrefixInt(const std::string&);
std::shared_ptr<std::string> removePrefixFloat(const std::string&);

std::shared_ptr<fpdb::expression::gandiva::Expression>
cascadeCast(std::shared_ptr<fpdb::expression::gandiva::Expression> expr);

}

#endif //FPDB_FPDB_EXPRESSION_GANDIVA_INCLUDE_FPDB_EXPRESSION_GANDIVA_EXPRESSION_H
