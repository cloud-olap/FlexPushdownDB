//
// Created by Yifei Yang on 7/27/20.
//

#include "normal/plan/mode/Mode.h"
#include <string>

using namespace normal::plan::operator_::mode;

Mode::Mode(ModeId id): id_(id) {}

bool Mode::is(std::shared_ptr<Mode> mode) {
  return id_ == mode->id_;
}

std::string Mode::toString() {
  switch(id_) {
    case ModeId::FullPullup: return "Full Pullup";
    case ModeId::FullPushdown: return "Full Pushdown";
    case ModeId::PullupCaching: return "Pullup Caching";
    case ModeId::HybridCaching: return "Hybrid Caching";
    case ModeId::HybridCachingLast: return "Hybrid Caching (Last)";
    default:
      /*
       * Shouldn't occur, but we'll throw a serious-ish exception if it ever does
       */
      throw std::domain_error("Cannot get string for mode '" + std::to_string(id_ )+ "'. Unrecognized mode");
  }
}

ModeId Mode::id() const {
  return id_;
}


