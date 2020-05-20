//
// Created by matt on 20/5/20.
//

#include "normal/core/cache/EvictRequestMessage.h"

#include <utility>

using namespace normal::core::cache;

EvictRequestMessage::EvictRequestMessage(std::shared_ptr<EvictionPolicyType> evictionPolicyType) :
	evictionPolicyType_(std::move(evictionPolicyType)) {}

[[maybe_unused]] const std::shared_ptr<EvictionPolicyType> &EvictRequestMessage::getEvictionPolicy() const {
  return evictionPolicyType_;
}

std::string EvictRequestMessage::toString() const {
  return fmt::format("evictionPolicyType: {}", evictionPolicyType_->toString());
}
