//
// Created by matt on 16/4/20.
//

#ifndef NORMAL_NORMAL_PLAN_INCLUDE_NORMAL_PLAN_OPERATORTYPEID_H
#define NORMAL_NORMAL_PLAN_INCLUDE_NORMAL_PLAN_OPERATORTYPEID_H

namespace normal::plan::operator_::type {

enum OperatorTypeId {
  Scan,
  Project,
  Aggregate,
  Collate,
  Join,
  Group
};

}

#endif //NORMAL_NORMAL_PLAN_INCLUDE_NORMAL_PLAN_OPERATORTYPEID_H
