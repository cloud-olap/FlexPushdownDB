//
// Created by matt on 20/5/20.
//

#ifndef NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_CACHE_EVICTREQUESTMESSAGE_H
#define NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_CACHE_EVICTREQUESTMESSAGE_H

#include <caf/all.hpp>

#include <normal/core/message/Message.h>

#include "EvictionPolicyType.h"

using namespace caf;
using namespace normal::core::message;

namespace normal::core::cache {

/**
 * Request for the segment cache actor to run the given eviction policy.
 */
class EvictRequestMessage : public Message {

public:
  EvictRequestMessage(std::shared_ptr<EvictionPolicyType> evictionPolicyType,
					  const std::string &sender);

  static std::shared_ptr<EvictRequestMessage> make(std::shared_ptr<EvictionPolicyType> evictionPolicyType,
												   const std::string &sender);

  [[maybe_unused]] [[nodiscard]] const std::shared_ptr<EvictionPolicyType> &getEvictionPolicy() const;

  [[nodiscard]] std::string toString() const;

private:
  std::shared_ptr<EvictionPolicyType> evictionPolicyType_;

};

}

CAF_ALLOW_UNSAFE_MESSAGE_TYPE(normal::core::cache::EvictRequestMessage)

#endif //NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_CACHE_EVICTREQUESTMESSAGE_H
