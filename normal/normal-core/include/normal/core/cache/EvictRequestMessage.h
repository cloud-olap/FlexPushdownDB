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
  explicit EvictRequestMessage(std::shared_ptr<EvictionPolicyType> EvictionPolicy);

  [[maybe_unused]] [[nodiscard]] const std::shared_ptr<EvictionPolicyType> &getEvictionPolicy() const;

  std::string toString();

private:
  std::shared_ptr<EvictionPolicyType> evictionPolicy_;

};

}

CAF_ALLOW_UNSAFE_MESSAGE_TYPE(normal::core::cache::EvictRequestMessage)

#endif //NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_CACHE_EVICTREQUESTMESSAGE_H
