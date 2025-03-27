//
// Created by Yifei Yang on 4/15/24.
//

#include <fpdb/executor/physical/join/DistJoinType.h>
#include <fmt/format.h>

namespace fpdb::executor::physical::join {

std::string DistJoinTypeUtil::toString(DistJoinType type) {
  switch (type) {
    case DistJoinType::BCAST: return "BCAST";
    case DistJoinType::PTION: return "PTION";
    case DistJoinType::COST_BASED_STATIC: return "COST_BASED_STATIC";
    case DistJoinType::COST_BASED_ADAPT: return "COST_BASED_ADAPT";
  }
}

std::string DistJoinTypeUtil::toJoinDigest(
  uint leftPrePOpId, uint rightPrePOpId, const std::string &hashJoinPredicateStr) {
  return fmt::format("{}-{}-{}", leftPrePOpId, rightPrePOpId, hashJoinPredicateStr);
}

DistJoinType DistJoinTypeUtil::optimize(int64_t leftRowCount, int64_t rightRowCount, int numNodes) {
  // left table should be the smaller one
  if (leftRowCount > rightRowCount) {
    throw std::runtime_error("Left table row count > right table row count.");
  }

  // currenly only have one cost func
  return optimizeNetwork(leftRowCount, rightRowCount, numNodes);
}

DistJoinType DistJoinTypeUtil::optimizeNetwork(int64_t leftRowCount, int64_t rightRowCount, int numNodes) {
  double bcastCost = 1.0 * leftRowCount * (numNodes - 1);
  double ptionCost = 1.0 * (leftRowCount + rightRowCount) / numNodes * (numNodes - 1);
  return bcastCost < ptionCost ? DistJoinType::BCAST : DistJoinType::PTION;
}

}
