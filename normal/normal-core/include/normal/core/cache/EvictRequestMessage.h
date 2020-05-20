//
// Created by matt on 20/5/20.
//

#ifndef NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_CACHE_EVICTREQUESTMESSAGE_H
#define NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_CACHE_EVICTREQUESTMESSAGE_H

#include <caf/all.hpp>
#include <fmt/format.h>

#include "EvictionPolicyType.h"

using namespace caf;

namespace normal::core::cache {

/**
 * Request for the segment cache actor to run the given eviction policy.
 */
class EvictRequestMessage {

public:
  explicit EvictRequestMessage(std::shared_ptr<EvictionPolicyType> evictionPolicyType);

  [[maybe_unused]] [[nodiscard]] const std::shared_ptr<EvictionPolicyType> &getEvictionPolicy() const;

  [[nodiscard]] std::string toString() const;

private:
  std::shared_ptr<EvictionPolicyType> evictionPolicyType_;

};

}

CAF_ALLOW_UNSAFE_MESSAGE_TYPE(normal::core::cache::EvictRequestMessage)

#endif //NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_CACHE_EVICTREQUESTMESSAGE_H
