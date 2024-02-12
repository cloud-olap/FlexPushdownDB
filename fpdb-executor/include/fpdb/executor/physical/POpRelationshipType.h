//
// Created by matt on 25/3/20.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_POPRELATIONSHIPTYPE_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_POPRELATIONSHIPTYPE_H

namespace fpdb::executor::physical {

/**
 * Represents the relationships physical operators can have with each other, that is either producing or consuming
 */
enum class POpRelationshipType {
  Producer,
  Consumer
};

}

#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_POPRELATIONSHIPTYPE_H
