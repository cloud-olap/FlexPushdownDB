//
// Created by matt on 6/5/20.
//

#ifndef NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_FILTER_H
#define NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_FILTER_H

#include <normal/expression/gandiva/Expression.h>
#include <gandiva/expression.h>
#include <gandiva/filter.h>
#include <normal/expression/Filter.h>
#include "Projector.h"

namespace normal::expression::gandiva {

class Filter : public normal::expression::Filter {

public:
  Filter(std::shared_ptr<Expression> Pred);

  static std::shared_ptr<Filter> make(const std::shared_ptr<Expression> &Pred);

  std::shared_ptr<normal::tuple::TupleSet2> evaluate(const normal::tuple::TupleSet2 &TupleSet) override;
  void compile(const std::shared_ptr<normal::tuple::Schema> &Schema) override;

private:
  std::shared_ptr<Expression> pred_;
  ::gandiva::ExpressionPtr gandivaExpression_;
  ::gandiva::ConditionPtr gandivaCondition_;
  std::shared_ptr<::gandiva::Projector> gandivaProjector_;
  std::shared_ptr<::gandiva::Filter> gandivaFilter_;
};

}

#endif //NORMAL_NORMAL_EXPRESSION_GANDIVA_INCLUDE_NORMAL_EXPRESSION_GANDIVA_FILTER_H
