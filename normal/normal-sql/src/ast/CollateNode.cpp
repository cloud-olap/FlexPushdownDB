//
// Created by matt on 1/4/20.
//

#include <normal/pushdown/Collate.h>
#include "CollateNode.h"
std::shared_ptr<normal::core::Operator> CollateNode::toOperator() {
  return std::make_shared<normal::pushdown::Collate>("collate");
}
