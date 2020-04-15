//
// Created by matt on 1/4/20.
//

#ifndef NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_LOGICAL_COLLATELOGICALOPERATOR_H
#define NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_LOGICAL_COLLATELOGICALOPERATOR_H

#include <memory>

#include <normal/core/Operator.h>
#include <normal/sql/logical/LogicalOperator.h>

namespace normal::sql::logical {

class CollateLogicalOperator : public LogicalOperator {
public:
  std::shared_ptr<core::Operator> toOperator() override;
};

}

#endif //NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_LOGICAL_COLLATELOGICALOPERATOR_H
