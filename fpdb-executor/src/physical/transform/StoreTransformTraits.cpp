//
// Created by Yifei Yang on 3/20/22.
//

#include <fpdb/executor/physical/transform/StoreTransformTraits.h>
#include <fpdb/executor/physical/Globals.h>

namespace fpdb::executor::physical {

StoreTransformTraits::StoreTransformTraits(const std::set<POpType> &addiSeparablePOpTypes,
                                           bool filterBitmapPushdownEnabled):
  addiSeparablePOpTypes_(addiSeparablePOpTypes),
  filterBitmapPushdownEnabled_(filterBitmapPushdownEnabled) {}

std::shared_ptr<StoreTransformTraits> StoreTransformTraits::S3StoreTransformTraits() {
  return std::make_shared<StoreTransformTraits>(std::set<POpType>{},
                                                false);
}

std::shared_ptr<StoreTransformTraits> StoreTransformTraits::FPDBStoreStoreTransformTraits() {
  std::set<POpType> separableOpTypes;
  if (ENABLE_GROUP_BY_PUSHDOWN) {
    separableOpTypes.emplace(GROUP);
  }
  if (ENABLE_SHUFFLE_PUSHDOWN) {
    separableOpTypes.emplace(SHUFFLE);
  }
  if (ENABLE_BLOOM_FILTER_PUSHDOWN) {
    separableOpTypes.emplace(BLOOM_FILTER_USE);
  }
  return std::make_shared<StoreTransformTraits>(separableOpTypes, ENABLE_FILTER_BITMAP_PUSHDOWN);
}

bool StoreTransformTraits::isSeparable(POpType pOpType) const {
  return addiSeparablePOpTypes_.find(pOpType) != addiSeparablePOpTypes_.end();
}

bool StoreTransformTraits::isFilterBitmapPushdownEnabled() const {
  return filterBitmapPushdownEnabled_;
}

}
