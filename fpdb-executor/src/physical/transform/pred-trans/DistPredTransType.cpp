//
// Created by Yifei Yang on 3/12/24.
//

#include <fpdb/executor/physical/transform/pred-trans/DistPredTransType.h>
#include <fpdb/executor/physical/Globals.h>
#include <fpdb/tuple/arrow/exec/BloomFilter.h>
#include <fpdb/tuple/util/BloomFilterMathUtil.h>
#include <fmt/format.h>
#include <limits>

namespace fpdb::executor::physical {

std::string DistPredTransTypeUtil::toString(DistPredTransType type) {
  switch (type) {
    case DistPredTransType::BCAST_VAL: return "BCAST_VAL";
    case DistPredTransType::BCAST_BF: return "BCAST_BF";
    case DistPredTransType::PTION_VAL: return "PTION_VAL";
    case DistPredTransType::PTION_SRC_VAL_DST_BF: return "PTION_SRC_VAL_DST_BF";
    case DistPredTransType::PTION_SRC_BF_DST_VAL: return "PTION_SRC_BF_DST_VAL";
    case DistPredTransType::SEMI_JOIN_RED: return "SEMI_JOIN_RED";
    case DistPredTransType::ADAPT: return "ADAPT";
  }
}

std::string DistPredTransTypeUtil::toStepDigest(
  const std::string &dirSbl, uint step, const std::string &hashJoinPredicateStr) {
  return fmt::format("{}-{}-{}", dirSbl, step, hashJoinPredicateStr);
}

bool DistPredTransTypeUtil::isDistPredTransBcast() {
  return DIST_PRED_TRANS_TYPE == DistPredTransType::BCAST_VAL || DIST_PRED_TRANS_TYPE == DistPredTransType::BCAST_BF;
}

DistPredTransType DistPredTransTypeUtil::optimize(
        int64_t numSrcRows, int64_t numDstRows, std::optional<int64_t> numDstRowsOut, double keyLen, int numNodes) {
  // network cost
  auto network = networkCost(numSrcRows, numDstRows, numDstRowsOut, keyLen, numNodes);

  // compute cost
  auto compute = computeCost(numSrcRows, numDstRows, numDstRowsOut, keyLen, numNodes);

  // return the one with min cost
  DistPredTransType minType;
  double minCost = std::numeric_limits<double>::max();
  for (const auto &it: network) {
    DistPredTransType type = it.first;
    double cost = it.second + (ADD_COMPUTE_COST ? compute[type] * COMPUTE_WT : 0.0);
    if (cost < minCost) {
      minType = type;
      minCost = cost;
    }
  }
  return minType;
}

std::unordered_map<DistPredTransType, double> DistPredTransTypeUtil::networkCost(
        int64_t numSrcRows, int64_t numDstRows, std::optional<int64_t> numDstRowsOut, double keyLen, int numNodes) {
  std::unordered_map<DistPredTransType, double> costs;
  double bfKeyLen = ((double) arrow::compute::BlockedBloomFilter::kMinNumBitsPerKey) / 8.0;
  double fpr = tuple::util::BloomFilterMathUtil::BloomFilterMathUtil::fpr(
          arrow::compute::BloomFilterMasks::kMaxBitsSet, arrow::compute::BlockedBloomFilter::kMinNumBitsPerKey);
  double bfKeyLenDist = tuple::util::BloomFilterMathUtil::BloomFilterMathUtil::numBitsPerKeyDist(
          arrow::compute::BloomFilterMasks::kMaxBitsSet, fpr, numNodes) / 8.0;

  // BCAST_VAL
  costs[DistPredTransType::BCAST_VAL] = 1.0 * (numNodes - 1) * keyLen * numSrcRows;

  // BCAST_BF
  costs[DistPredTransType::BCAST_BF] = 2.0 * (numNodes - 1) * bfKeyLen * numSrcRows;

  // PTION_VAL, first compute network during partitioning, then compute network when sending back filtered join keys
  double cost = keyLen * (numSrcRows + numDstRows);
  if (numDstRowsOut.has_value()) {
    // fpr = (numDstRowsOut - x) / (numDstRows - x), x is num join keys sent back after probe
    // (*numDstRowsOut - numDstRows * fpr) may be negative if real achieved fpr is much less than computed above,
    // (e.g., duplicate keys inserted into BF), we exclude if this happens
    cost += keyLen * (std::max(0.0, *numDstRowsOut - numDstRows * fpr) / (1 - fpr));
  } else {
    cost += keyLen * (SEL_DST + fpr * (1 - SEL_DST)) * numDstRows;
  }
  cost *= 1.0 * (numNodes - 1) / numNodes;    // one partition of n is sent to self
  costs[DistPredTransType::PTION_VAL] = cost;

  // PTION_SRC_BF_DST_VAL, in two parts similar to PTION_VAL
  cost = bfKeyLenDist * numSrcRows + keyLen * numDstRows;
  if (numDstRowsOut.has_value()) {
    // similar to PTION_VAL
    cost += bfKeyLen * (std::max(0.0, *numDstRowsOut - numDstRows * fpr) / (1 - fpr));
  } else {
    cost += bfKeyLen * (SEL_DST + fpr * (1 - SEL_DST)) * numDstRows;
  }
  cost *= 1.0 * (numNodes - 1) / numNodes;    // one partition of n is sent to self
  costs[DistPredTransType::PTION_SRC_BF_DST_VAL] = cost;

  // PTION_SRC_VAL_DST_BF, in two parts similar to PTION_VAL
  cost = keyLen * numSrcRows + bfKeyLen * numDstRows;
  if (numDstRowsOut.has_value()) {
    // join keys belonging to dst side, same as PTION_SRC_BF_DST_VAL
    double n1 = std::max(0.0, *numDstRowsOut - numDstRows * fpr) / (1 - fpr);
    // join keys belonging to src side, since we use src to probe dst BF, so there will be FP from src side
    double n2 = fpr * (numNodes * numSrcRows - n1);
    // add together
    cost += bfKeyLen * (n1 + n2);
  } else {
    cost += bfKeyLen * (SEL_DST * numDstRows + fpr * (1 - SEL_SRC / numNodes) * numSrcRows * numNodes);
  }
  cost *= 1.0 * (numNodes - 1) / numNodes;    // one partition of n is sent to self
  costs[DistPredTransType::PTION_SRC_VAL_DST_BF] = cost;

  return costs;
}

std::unordered_map<DistPredTransType, double> DistPredTransTypeUtil::computeCost(
  int64_t numSrcRows, int64_t numDstRows, std::optional<int64_t> numDstRowsOut, double keyLen, int numNodes) {
  std::unordered_map<DistPredTransType, double> costs;
  double fpr = tuple::util::BloomFilterMathUtil::BloomFilterMathUtil::fpr(
    arrow::compute::BloomFilterMasks::kMaxBitsSet, arrow::compute::BlockedBloomFilter::kMinNumBitsPerKey);

  // BCAST_VAL
  costs[DistPredTransType::BCAST_VAL] = BF_WT * (numSrcRows * numNodes + numDstRows) * keyLen;

  // BCAST_BF
  costs[DistPredTransType::BCAST_BF] = BF_WT * (numSrcRows + numDstRows) * keyLen;

  // PTION_VAL, two parts --- bf cost (both build and probe) and shuffle cost
  // bf cost contains one when src key reduces dst key and another when reduced dst key reduces dst table
  double bfCost = BF_WT * (numSrcRows + numDstRows) * keyLen;
  if (numDstRowsOut.has_value()) {
    bfCost += BF_WT * (std::max(0.0, *numDstRowsOut - numDstRows * fpr) / (1 - fpr) + numDstRows) * keyLen;
  } else {
    bfCost += BF_WT * (SEL_DST + fpr * (1 - SEL_DST) + 1) * numDstRows * keyLen;
  }
  double shuffleCost = SF_WT * (numSrcRows + numDstRows) * keyLen;
  costs[DistPredTransType::PTION_VAL] = bfCost + shuffleCost;

  // PTION_SRC_BF_DST_VAL, difference from PTION_VAL is that each probe actually visits "numNodes" BFs
  bfCost += BF_WT * numDstRows * (numNodes - 1) * keyLen;
  costs[DistPredTransType::PTION_SRC_BF_DST_VAL] = bfCost + shuffleCost;

  // PTION_SRC_VAL_DST_BF
  bfCost = BF_WT * (numSrcRows * numNodes + numDstRows) * keyLen;
  if (numDstRowsOut.has_value()) {
    // join keys belonging to dst side, same as PTION_SRC_BF_DST_VAL
    double n1 = std::max(0.0, *numDstRowsOut - numDstRows * fpr) / (1 - fpr);
    // join keys belonging to src side, since we use src to probe dst BF, so there will be FP from src side
    double n2 = fpr * (numNodes * numSrcRows - n1);
    // final cost of the portion
    bfCost += BF_WT * (n1 + n2 + numDstRows) * keyLen;
  } else {
    bfCost += BF_WT * ((SEL_DST * numDstRows + fpr * (1 - SEL_SRC / numNodes) * numSrcRows * numNodes) + numDstRows)
              * keyLen;
  }
  costs[DistPredTransType::PTION_SRC_VAL_DST_BF] = bfCost + shuffleCost;

  return costs;
}

}
