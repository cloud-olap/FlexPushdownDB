//
// Created by matt on 20/5/20.
//

#ifndef NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_CACHE_STOREREQUESTMESSAGE_H
#define NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_CACHE_STOREREQUESTMESSAGE_H

#include <normal/core/message/Message.h>

#include <normal/cache/SegmentKey.h>
#include <normal/cache/SegmentData.h>

using namespace normal::cache;
using namespace normal::core::message;

namespace normal::core::cache {

/**
 * Reuest for the segment cache actor to store the given segment data given a segment key.
 */
class StoreRequestMessage : public Message {

public:
  StoreRequestMessage(std::unordered_map<std::shared_ptr<SegmentKey>, std::shared_ptr<SegmentData>> segments,
					  const std::string &sender);

  static std::shared_ptr<StoreRequestMessage>
  make(std::unordered_map<std::shared_ptr<SegmentKey>, std::shared_ptr<SegmentData>> segments,
	   const std::string &sender);

  [[nodiscard]] const std::unordered_map<std::shared_ptr<SegmentKey>, std::shared_ptr<SegmentData>> &
  getSegments() const;

  [[nodiscard]] std::string toString() const;

private:
  std::unordered_map<std::shared_ptr<SegmentKey>, std::shared_ptr<SegmentData>> segments_;
};

}

#endif //NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_CACHE_STOREREQUESTMESSAGE_H
