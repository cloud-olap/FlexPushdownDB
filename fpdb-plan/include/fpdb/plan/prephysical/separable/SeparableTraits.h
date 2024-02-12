//
// Created by Yifei Yang on 2/26/22.
//

#ifndef FPDB_FPDB_PLAN_INCLUDE_FPDB_PLAN_PREPHYSICAL_SEPARABLE_SEPARABLETRAITS_H
#define FPDB_FPDB_PLAN_INCLUDE_FPDB_PLAN_PREPHYSICAL_SEPARABLE_SEPARABLETRAITS_H

#include <fpdb/plan/prephysical/PrePOpType.h>
#include <set>
#include <memory>

namespace fpdb::plan::prephysical::separable {

/**
 * Denotes what operators are separable, with regard to the underlying store
 */
class SeparableTraits {

public:
  SeparableTraits(const std::set<PrePOpType> &separablePrePOpTypes);

  static std::shared_ptr<SeparableTraits> S3SeparableTraits();
  static std::shared_ptr<SeparableTraits> FPDBStoreSeparableTraits();
  static std::shared_ptr<SeparableTraits> localFSSeparableTraits();
  static std::shared_ptr<SeparableTraits> unknownStoreSeparableTraits();

  bool isSeparable(PrePOpType prePOpType) const;

private:
  std::set<PrePOpType> separablePrePOpTypes_;

};

}


#endif //FPDB_FPDB_PLAN_INCLUDE_FPDB_PLAN_PREPHYSICAL_SEPARABLE_SEPARABLETRAITS_H
