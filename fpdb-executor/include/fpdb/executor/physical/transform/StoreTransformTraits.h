//
// Created by Yifei Yang on 3/20/22.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_TRANSFORM_STORETRANSFORMTRAITS_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_TRANSFORM_STORETRANSFORMTRAITS_H

#include <fpdb/executor/physical/PhysicalOp.h>
#include <set>

namespace fpdb::executor::physical {

/**
 * Denotes what additional physical operators can be pushed to what underlying store
 * The ones already in `fpdb::plan::separable::SeparableTraits` are not considered here,
 * here we only consider "additional" physical operators which haven't been pushed in prephysical phase,
 * which currently are `bloom filter`, `shuffle`.
 */
class StoreTransformTraits {

public:
  StoreTransformTraits(const std::set<POpType> &addiSeparablePOpTypes,
                       bool filterBitmapPushdownEnabled);

  static std::shared_ptr<StoreTransformTraits> S3StoreTransformTraits();
  static std::shared_ptr<StoreTransformTraits> FPDBStoreStoreTransformTraits();

  bool isSeparable(POpType pOpType) const;
  bool isFilterBitmapPushdownEnabled() const;

private:
  std::set<POpType> addiSeparablePOpTypes_;
  bool filterBitmapPushdownEnabled_;

};

}


#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_TRANSFORM_STORETRANSFORMTRAITS_H
