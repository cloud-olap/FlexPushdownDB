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
    case ModeId::FullPushdown: return "full pushdown";
    case ModeId::PullupCaching: return "pull up caching";
    case ModeId::HybridCaching: return "hybrid caching";
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


