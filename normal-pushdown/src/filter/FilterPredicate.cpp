//
// Created by matt on 6/5/20.
//

#include "normal/pushdown/filter/FilterPredicate.h"

#include <utility>

using namespace normal::pushdown::filter;

[[maybe_unused]] FilterPredicate::FilterPredicate(std::shared_ptr<normal::expression::gandiva::Expression> Expr) :
	expr_(std::move(Expr)) {}

std::shared_ptr<FilterPredicate> FilterPredicate::make(const std::shared_ptr<normal::expression::gandiva::Expression>& Expr) {
  return std::make_shared<FilterPredicate>(Expr);
}

const std::shared_ptr<normal::expression::gandiva::Expression> &FilterPredicate::expression() const {
  return expr_;
}




