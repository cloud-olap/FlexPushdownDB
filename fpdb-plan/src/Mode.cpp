//
// Created by Yifei Yang on 7/27/20.
//

#include <fpdb/plan/Mode.h>
#include <fmt/format.h>

namespace fpdb::plan {

Mode::Mode(ModeId id): id_(id) {}

ModeId Mode::id() const {
  return id_;
}

std::string Mode::toString() {
  switch(id_) {
    case PULL_UP: return "Pullup";
    case PUSHDOWN_ONLY: return "Pushdown-only";
    case CACHING_ONLY: return "Caching-only";
    case HYBRID: return "Hybrid";
    default:
      throw std::runtime_error(fmt::format("Unknown mode, id: {}", id_));
  }
}

bool Mode::is(const std::shared_ptr<Mode>& mode) {
  return id_ == mode->id_;
}

std::shared_ptr<Mode> Mode::pullupMode() {
  return std::make_shared<Mode>(PULL_UP);
}

std::shared_ptr<Mode> Mode::pushdownOnlyMode() {
  return std::make_shared<Mode>(PUSHDOWN_ONLY);
}

std::shared_ptr<Mode> Mode::cachingOnlyMode() {
  return std::make_shared<Mode>(CACHING_ONLY);
}

std::shared_ptr<Mode> Mode::hybridMode() {
  return std::make_shared<Mode>(HYBRID);
}

}
