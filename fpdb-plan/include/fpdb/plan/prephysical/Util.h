//
// Created by Yifei Yang on 4/21/23.
//

#ifndef FPDB_FPDB_PLAN_INCLUDE_FPDB_PLAN_PREPHYSICAL_UTIL_H
#define FPDB_FPDB_PLAN_INCLUDE_FPDB_PLAN_PREPHYSICAL_UTIL_H

#include <fpdb/plan/prephysical/PrePhysicalOp.h>
#include <optional>

namespace fpdb::plan::prephysical {

class Util {

public:
  /**
   * Find the original ID of the scan op that gives input to this op
   * @param op
   * @return scan op ID if no join occurs between the scan op and this op, otherwise nullopt
   */
  static std::optional<uint> traceScanOriginWithNoJoinInPath(const std::shared_ptr<PrePhysicalOp> &op);

  /**
   * Find all prephysical ops under the root op of the given type
   * @param op the root op
   * @return
   */
  static std::vector<std::shared_ptr<PrePhysicalOp>> findAllOfType(const std::shared_ptr<PrePhysicalOp> &op,
                                                                   PrePOpType type);
};

}


#endif //FPDB_FPDB_PLAN_INCLUDE_FPDB_PLAN_PREPHYSICAL_UTIL_H
