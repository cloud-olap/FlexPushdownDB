//
// Created by matt on 28/4/20.
//

#include "normal/expression/gandiva/Add.h"

using namespace normal::expression::gandiva;

Add::Add(std::shared_ptr<Expression> left, std::shared_ptr<Expression> right)
	: left_(std::move(left)), right_(std::move(right)) {
}

void Add::compile(std::shared_ptr<arrow::Schema> /* schema */) {
  // TODO
}

std::string Add::alias() {
  return "?column?";
}

std::shared_ptr<Expression> normal::expression::gandiva::plus(std::shared_ptr<Expression> left,
															  std::shared_ptr<Expression> right) {
  return std::make_shared<Add>(std::move(left), std::move(right));
}
