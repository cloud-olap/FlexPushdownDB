//
// Created by matt on 20/5/20.
//

#ifndef NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_CACHE_EVICTIONPOLICYTYPE_H
#define NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_CACHE_EVICTIONPOLICYTYPE_H

#include <string>
#include <stdexcept>
#include <memory>

namespace normal::core::cache {

enum EvictionPolicyTypeId {
  LRU,
  LFU
  // TODO: Probably others
};

/**
 * An eviction policy like LRU, LFU, etc.
 *
 * Not an implementation of the policy just the type
 */
class EvictionPolicyType {

public:
  explicit EvictionPolicyType(EvictionPolicyTypeId Id);

  static std::shared_ptr<EvictionPolicyType> make(EvictionPolicyTypeId Id);

  [[maybe_unused]] [[nodiscard]] EvictionPolicyTypeId getId() const;

  std::string toString();

private:
  EvictionPolicyTypeId id_;

};

}

#endif //NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_CACHE_EVICTIONPOLICYTYPE_H
