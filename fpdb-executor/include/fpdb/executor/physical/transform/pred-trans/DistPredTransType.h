//
// Created by Yifei Yang on 3/12/24.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_TRANSFORM_PRED_TRANS_DISTPREDTRANSTYPE_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_TRANSFORM_PRED_TRANS_DISTPREDTRANSTYPE_H

#include <optional>
#include <sys/types.h>
#include <unordered_map>

namespace fpdb::executor::physical {

enum class DistPredTransType {
  BCAST_VAL,
  BCAST_BF,
  PTION_VAL,
  PTION_SRC_BF_DST_VAL,
  PTION_SRC_VAL_DST_BF,
  SEMI_JOIN_RED,  /* Traditional distributed semi-join reduction, i.e. BCAST_VAL using semi-join instead of BF */
  ADAPT
};

class DistPredTransTypeUtil {

public:
  static std::string toString(DistPredTransType type);

  static std::string toStepDigest(const std::string &dirSbl, uint step, const std::string &hashJoinPredicateStr);

  static bool isDistPredTransBcast();

  static DistPredTransType optimize(int64_t numSrcRows, int64_t numDstRows,
                                    std::optional<int64_t> numDstRowsOut,
                                    double keyLen, int numNodes);

private:
  // when we don't have numDstRowsOut collected (e.g., w/o "USE_DOUBLE_EXEC_ADAPT"), we don't know sel of src/dst
  // during transfer, here we assume fixed consts
  static constexpr double SEL_SRC = 0.2;
  static constexpr double SEL_DST = 0.1;

  // weights assigned to different computation tasks, with respect to a network cost unit
  // here we just count the number of bf and shuffle operations
  static constexpr bool ADD_COMPUTE_COST = false;
  static constexpr double COMPUTE_WT = 0.05;     // relative over network cost
  static constexpr double BF_WT = 1;
  static constexpr double SF_WT = 1;

  // use num bytes as cost unit
  static std::unordered_map<DistPredTransType, double> networkCost(int64_t numSrcRows, int64_t numDstRows,
                                                                   std::optional<int64_t> numDstRowsOut,
                                                                   double keyLen, int numNodes);


  // use num bytes as cost unit
  static std::unordered_map<DistPredTransType, double> computeCost(int64_t numSrcRows, int64_t numDstRows,
                                                                   std::optional<int64_t> numDstRowsOut,
                                                                   double keyLen, int numNodes);
};

}

#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_TRANSFORM_PRED_TRANS_DISTPREDTRANSTYPE_H
