//
// Created by Yifei Yang on 4/15/24.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_JOIN_DISTJOINTYPE_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_JOIN_DISTJOINTYPE_H

#include <sys/types.h>
#include <string>

namespace fpdb::executor::physical::join {

enum class DistJoinType {
  BCAST,
  PTION,
  COST_BASED_STATIC,
  COST_BASED_ADAPT
};

class DistJoinTypeUtil {

public:
  static std::string toString(DistJoinType type);

  static std::string toJoinDigest(uint leftPrePOpId, uint rightPrePOpId, const std::string &hashJoinPredicateStr);

  static DistJoinType optimize(int64_t leftRowCount, int64_t rightRowCount, int numNodes);

private:
  static DistJoinType optimizeNetwork(int64_t leftRowCount, int64_t rightRowCount, int numNodes);
};

}

#endif // FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_JOIN_DISTJOINTYPE_H
