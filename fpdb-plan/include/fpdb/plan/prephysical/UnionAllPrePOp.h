//
// Created by Yifei Yang on 12/11/24.
//

#ifndef FPDB_FPDB_PLAN_INCLUDE_FPDB_PLAN_PREPHYSICAL_UNIONALLPREPOP_H
#define FPDB_FPDB_PLAN_INCLUDE_FPDB_PLAN_PREPHYSICAL_UNIONALLPREPOP_H

#include <fpdb/plan/prephysical/PrePhysicalOp.h>

namespace fpdb::plan::prephysical {

class UnionAllPrePOp: public PrePhysicalOp {
public:
  UnionAllPrePOp(uint id, double rowCount);

  string getTypeString() override;
  set<string> getUsedColumnNames() override;

private:
  bool equalTo(const std::shared_ptr<PrePhysicalOp> &other) const override;
};

}

#endif //FPDB_FPDB_PLAN_INCLUDE_FPDB_PLAN_PREPHYSICAL_UNIONALLPREPOP_H
