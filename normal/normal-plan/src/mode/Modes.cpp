//
// Created by Yifei Yang on 7/27/20.
//

#include "normal/plan/mode/Modes.h"

using namespace normal::plan::operator_::mode;

std::shared_ptr<FullPushdownMode> normal::plan::operator_::mode::Modes::fullPushdownMode() {
  return std::make_shared<FullPushdownMode>();
}

std::shared_ptr<PullupCachingMode> normal::plan::operator_::mode::Modes::pullupCachingMode() {
  return std::make_shared<PullupCachingMode>();
}

std::shared_ptr<HybridCachingMode> normal::plan::operator_::mode::Modes::hybridCachingMode() {
  return std::make_shared<HybridCachingMode>();
}
