//
// Created by matt on 20/5/20.
//

#include "normal/core/cache/EvictRequestMessage.h"

#include <utility>

using namespace normal::core::cache;

EvictRequestMessage::EvictRequestMessage(std::shared_ptr<EvictionPolicyType> EvictionPolicy) : evictionPolicy_(std::move(EvictionPolicy)) {}

[[maybe_unused]] const std::shared_ptr<EvictionPolicyType> &EvictRequestMessage::getEvictionPolicy() const {
  return evictionPolicy_;
}

std::string EvictRequestMessage::toString() {
  return fmt::format("evictionPolicy: {}", evictionPolicy_->toString());
}
