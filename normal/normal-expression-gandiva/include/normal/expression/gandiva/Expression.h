//
// Created by matt on 27/4/20.
//

#ifndef NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_EXPRESSION_H
#define NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_EXPRESSION_H

#include <memory>

#include <arrow/type.h>
#include <gandiva/node.h>

#include <normal/expression/Expression.h>

namespace normal::expression::gandiva {

class Expression : public normal::expression::Expression {

public:
  virtual ~Expression() = default;

  [[nodiscard]] virtual std::string alias() = 0;

  [[nodiscard]] virtual std::shared_ptr<std::vector<std::string>> involvedColumnNames() = 0;

  [[nodiscard]] const ::gandiva::NodePtr &getGandivaExpression() const;

  std::string showString();

protected:
  ::gandiva::NodePtr gandivaExpression_;

};

const inline std::string prefixInt_ = "int:";
const inline std::string prefixFloat_ = "float:";
std::shared_ptr<std::string> removePrefixInt(std::string);
std::shared_ptr<std::string> removePrefixFloat(std::string);

// FIXME: only applicable for ssb queries
std::shared_ptr<normal::expression::gandiva::Expression> simpleCast(std::shared_ptr<normal::expression::gandiva::Expression> expr);

}

#endif //NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_EXPRESSION_H
