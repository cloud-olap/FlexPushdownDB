//
// Created by matt on 20/5/20.
//

#include "normal/core/cache/EvictionPolicyType.h"

using namespace normal::core::cache;

EvictionPolicyType::EvictionPolicyType(EvictionPolicyTypeId Id) : id_(Id) {}

std::shared_ptr<EvictionPolicyType> EvictionPolicyType::make(EvictionPolicyTypeId Id) {
  return std::make_shared<EvictionPolicyType>(Id);
}

[[maybe_unused]] EvictionPolicyTypeId EvictionPolicyType::getId() const {
  return id_;
}

std::string EvictionPolicyType::toString() {
  switch (id_) {
  case EvictionPolicyTypeId::LRU: return "LRU";
  case EvictionPolicyTypeId::LFU: return "LFU";
  default: throw std::runtime_error("Unrecognized eviction policy id");
  }
}

