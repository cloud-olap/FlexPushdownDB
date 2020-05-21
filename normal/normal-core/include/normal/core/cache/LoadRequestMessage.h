//
// Created by matt on 20/5/20.
//

#ifndef NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_CACHE_LOADREQUESTMESSAGE_H
#define NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_CACHE_LOADREQUESTMESSAGE_H

#include <caf/all.hpp>
#include <normal/core/message/Message.h>

#include <normal/cache/SegmentKey.h>
#include <normal/cache/SegmentData.h>

using namespace caf;
using namespace normal::cache;
using namespace normal::core::message;

namespace normal::core::cache {

/**
 * Request for the segment cache actor to load a segment given a segment key. On a hit, the segment is sent to the
 * requesting actor, or nullopt on a cache miss.
 */
class LoadRequestMessage : public Message {

public:
  LoadRequestMessage(std::shared_ptr<SegmentKey> segmentKey,
							  const std::string &sender);

  static std::shared_ptr<LoadRequestMessage> make(std::shared_ptr<SegmentKey> segmentKey, const std::string &sender);

  [[nodiscard]] const std::shared_ptr<SegmentKey> &getSegmentKey() const;

  [[nodiscard]] std::string toString() const;

private:
  std::shared_ptr<SegmentKey> segmentKey_;

};

}

CAF_ALLOW_UNSAFE_MESSAGE_TYPE(normal::core::cache::LoadRequestMessage)

#endif //NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_CACHE_LOADREQUESTMESSAGE_H
