//
// Created by Yifei Yang on 7/14/20.
//

#ifndef NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_LOGICAL_JOINLOGICALOPERATOR_H
#define NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_LOGICAL_JOINLOGICALOPERATOR_H

#include <string>
#include <normal/expression/gandiva/Expression.h>
#include <normal/plan/operator_/LogicalOperator.h>

namespace normal::plan::operator_ {

class JoinLogicalOperator : public LogicalOperator{

public:
  explicit JoinLogicalOperator(const std::string &leftColumnName, const std::string &rightColumnName);

private:
  std::string leftColumnName_;
  std::string rightColumnName_;

};

}


#endif //NORMAL_JOINLOGICALOPERATOR_H
