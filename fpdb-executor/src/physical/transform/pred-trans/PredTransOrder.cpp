//
// Created by Yifei Yang on 5/15/23.
//

#include <fpdb/executor/physical/transform/pred-trans/SmallToLargePredTransOrder.h>
#include <fpdb/executor/physical/transform/pred-trans/BFSPredTransOrder.h>
#include <fpdb/executor/physical/Globals.h>
#include <fmt/format.h>

namespace fpdb::executor::physical {

void PredTransOrder::orderPredTrans(
        PredTransOrderType type,
        PrePToPTransformerForPredTrans* transformer,
        const std::unordered_set<std::shared_ptr<JoinOrigin>, JoinOriginPtrHash, JoinOriginPtrPred> &joinOrigins) {
  std::shared_ptr<PredTransOrder> predTransOrder;
  switch (type) {
    case PredTransOrderType::SMALL_TO_LARGE: {
      predTransOrder = std::make_shared<SmallToLargePredTransOrder>(transformer);
      break;
    }
    case PredTransOrderType::BFS: {
      predTransOrder = std::make_shared<BFSPredTransOrder>(transformer, ENABLE_YANNAKAKIS);
      break;
    }
    default: {
      throw std::runtime_error(fmt::format("Unknown PredTransOrderType: '{}'", type));
    }
  }
  predTransOrder->orderPredTrans(joinOrigins);
}

PredTransOrder::PredTransOrder(PredTransOrderType type,
                               PrePToPTransformerForPredTrans* transformer):
  type_(type),
  transformer_(transformer) {}

PredTransOrderType PredTransOrder::getType() const {
  return type_;
}

void PredTransOrder::updateTransRes() {
  // update the transform result of FilterableScanPrePOp, if it participates in predicate transfer
  for (auto &transResIt: transformer_->prePOpToTransRes_) {
    bool needToUpdate = true;
    std::vector<std::shared_ptr<PhysicalOp>> updatedTransRes;
    for (const auto &origConnOp: transResIt.second) {
      const auto &origUpConnOpToPTUnitIt = origUpConnOpToPTUnit_.find(origConnOp->name());
      if (origUpConnOpToPTUnitIt != origUpConnOpToPTUnit_.end()) {
        updatedTransRes.emplace_back(origUpConnOpToPTUnitIt->second->currUpConnOp_);
      } else {
        // no participation if predicate transfer
        needToUpdate = false;
        break;
      }
    }
    if (needToUpdate) {
      transResIt.second = updatedTransRes;
    }
  }
}

}
