//
// Created by matt on 6/5/20.
//

#ifndef NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_FILTER_FILTERPREDICATE_H
#define NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_FILTER_FILTERPREDICATE_H

#include <memory>

#include <normal/expression/gandiva/Expression.h>

namespace normal::pushdown::filter {

class FilterPredicate {

public:
  explicit FilterPredicate(std::shared_ptr<normal::expression::gandiva::Expression> Expr);

  static std::shared_ptr<FilterPredicate> make(const std::shared_ptr<normal::expression::gandiva::Expression>& Expr);

  const std::shared_ptr<normal::expression::gandiva::Expression> &expression() const;

private:
  std::shared_ptr<normal::expression::gandiva::Expression> expr_;

};

}

#endif //NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_FILTER_FILTERPREDICATE_H
