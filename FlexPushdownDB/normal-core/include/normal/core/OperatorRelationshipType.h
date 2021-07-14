//
// Created by matt on 25/3/20.
//

#ifndef NORMAL_NORMAL_CORE_SRC_OPERATORRELATIONSHIPTYPE_H
#define NORMAL_NORMAL_CORE_SRC_OPERATORRELATIONSHIPTYPE_H

namespace normal::core {

/**
 * Represents the relationships operators can have with each other, that is either producing or consuming
 */
enum class OperatorRelationshipType {
  Producer, Consumer, None
};

}

#endif //NORMAL_NORMAL_CORE_SRC_OPERATORRELATIONSHIPTYPE_H
