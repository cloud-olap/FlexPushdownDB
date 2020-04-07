//
// Created by matt on 1/4/20.
//

#include "logical/CollateLogicalOperator.h"

#include <normal/pushdown/Collate.h>

std::shared_ptr<normal::core::Operator> CollateLogicalOperator::toOperator() {
  return std::make_shared<normal::pushdown::Collate>("collate");
}
