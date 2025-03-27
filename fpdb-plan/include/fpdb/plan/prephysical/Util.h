//
// Created by Yifei Yang on 4/21/23.
//

#ifndef FPDB_FPDB_PLAN_INCLUDE_FPDB_PLAN_PREPHYSICAL_UTIL_H
#define FPDB_FPDB_PLAN_INCLUDE_FPDB_PLAN_PREPHYSICAL_UTIL_H

#include <fpdb/plan/prephysical/PrePhysicalOp.h>
#include <fpdb/plan/prephysical/FilterableScanPrePOp.h>
#include <optional>

namespace fpdb::plan::prephysical {

class Util {

public:
  /**
   * Find the original scan op that gives input to this op
   * @param op
   * @return scan op if no join occurs between the scan op and this op, otherwise nullptr
   */
  static std::shared_ptr<FilterableScanPrePOp> traceScanOriginWithNoJoinInPath(const std::shared_ptr<PrePhysicalOp> &op);

  /**
   * Check if it contains local filter that can potentially reduce scan cardinality on "key",
   * the local filter broadly contains group-by, limit, ...
   * @param op
   * @return
   */
  static bool hasLocalFilter(const std::shared_ptr<PrePhysicalOp> &op, const std::vector<std::string> &key);

  /**
   * Find all prephysical ops under the root op of the given type
   * @param op the root op
   * @return
   */
  static std::vector<std::shared_ptr<PrePhysicalOp>> findAllOfType(const std::shared_ptr<PrePhysicalOp> &op,
                                                                   PrePOpType type);

  /**
   * Get the base table or a short description how it derives from the base table
   * @param op
   * @return
   */
  static std::string getBaseTableDigest(const std::shared_ptr<PrePhysicalOp> &op);
};

}


#endif //FPDB_FPDB_PLAN_INCLUDE_FPDB_PLAN_PREPHYSICAL_UTIL_H
