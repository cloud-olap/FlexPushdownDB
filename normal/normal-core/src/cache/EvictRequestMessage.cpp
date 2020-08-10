//
// Created by matt on 20/5/20.
//

#include "normal/core/cache/EvictRequestMessage.h"

#include <utility>

#include <fmt/format.h>

using namespace normal::core::cache;

EvictRequestMessage::EvictRequestMessage(std::shared_ptr<EvictionPolicyType> evictionPolicyType,
										 const std::string &sender) :
	Message("EvictRequestMessage", sender),
	evictionPolicyType_(std::move(evictionPolicyType)) {}

std::shared_ptr<EvictRequestMessage> EvictRequestMessage::make(std::shared_ptr<EvictionPolicyType> evictionPolicyType,
															   const std::string &sender) {
  return std::make_shared<EvictRequestMessage>(std::move(evictionPolicyType), sender);
}

[[maybe_unused]] const std::shared_ptr<EvictionPolicyType> &EvictRequestMessage::getEvictionPolicy() const {
  return evictionPolicyType_;
}

std::string EvictRequestMessage::toString() const {
  return fmt::format("evictionPolicyType: {}", evictionPolicyType_->toString());
}

