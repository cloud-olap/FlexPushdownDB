//
// Created by Yifei Yang on 7/27/20.
//

#include "normal/plan/mode/Modes.h"

using namespace normal::plan::operator_::mode;

std::shared_ptr<FullPullupMode> Modes::fullPullupMode() {
  return std::make_shared<FullPullupMode>();
}

std::shared_ptr<FullPushdownMode> Modes::fullPushdownMode() {
  return std::make_shared<FullPushdownMode>();
}

std::shared_ptr<PullupCachingMode> Modes::pullupCachingMode() {
  return std::make_shared<PullupCachingMode>();
}

std::shared_ptr<HybridCachingMode> Modes::hybridCachingMode() {
  return std::make_shared<HybridCachingMode>();
}

std::shared_ptr<HybridCachingLastMode> Modes::hybridCachingLastMode() {
  return std::make_shared<HybridCachingLastMode>();
}
