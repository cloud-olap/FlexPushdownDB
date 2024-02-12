//
// Created by Yifei Yang on 10/31/21.
//

#ifndef FPDB_FPDB_PLAN_INCLUDE_FPDB_PLAN_PREPHYSICALPLAN_H
#define FPDB_FPDB_PLAN_INCLUDE_FPDB_PLAN_PREPHYSICALPLAN_H

#include <fpdb/plan/prephysical/PrePhysicalOp.h>

namespace fpdb::plan::prephysical {

class PrePhysicalPlan {
public:
  PrePhysicalPlan(const shared_ptr<PrePhysicalOp> &rootOp,
                  const vector<string> &outputColumnNames);

  const shared_ptr<PrePhysicalOp> &getRootOp() const;
  void setRootOp(const shared_ptr<PrePhysicalOp> &rootOp);
  const vector<string> &getOutputColumnNames() const;

  void populateAndTrimProjectColumns();

private:
  /**
   * Populate project columns for those that are not set.
   * (Project columns are set for some (not all) ops during deserialization (when deserializing Project)).
   */
  set<string> populateProjectColumnsDfs(const shared_ptr<PrePhysicalOp>& op);

  /**
   * Trim project columns.
   */
  void trimProjectColumnsDfs(const shared_ptr<PrePhysicalOp>& op,
                             const std::optional<set<string>> &optDownUsedColumns);

  shared_ptr<PrePhysicalOp> rootOp_;
  vector<string> outputColumnNames_;
};

}


#endif //FPDB_FPDB_PLAN_INCLUDE_FPDB_PLAN_PREPHYSICALPLAN_H
