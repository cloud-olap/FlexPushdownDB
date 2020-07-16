//
// Created by Yifei Yang on 7/14/20.
//

#include "normal/plan/operator_/JoinLogicalOperator.h"
#include <normal/plan/operator_/type/OperatorTypes.h>

using namespace normal::plan::operator_;

JoinLogicalOperator::JoinLogicalOperator(const std::string &leftColumnName, const std::string &rightColumnName)
        : LogicalOperator(type::OperatorTypes::joinOperatorType()),
          leftColumnName_(leftColumnName), rightColumnName_(rightColumnName) {}

