//
// Created by matt on 20/5/20.
//

#ifndef NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_CACHE_LOADRESPONSEMESSAGE_H
#define NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_CACHE_LOADRESPONSEMESSAGE_H

#include <caf/all.hpp>
#include <utility>

#include <normal/cache/SegmentKey.h>
#include <normal/cache/SegmentData.h>

#include <normal/core/message/Message.h>

using namespace caf;
using namespace normal::cache;
using namespace normal::core::message;

namespace normal::core::cache {

/**
 * Response sent from segment cache actor containing a segment loaded from cache, segment data is nullopt if segment was not found
 * in cache.
 */
class LoadResponseMessage : public Message {

public:
  LoadResponseMessage(std::unordered_map<std::shared_ptr<SegmentKey>, std::shared_ptr<SegmentData>> segments,
					  const std::string &sender);

  static std::shared_ptr<LoadResponseMessage> make(
  	std::unordered_map<std::shared_ptr<SegmentKey>, std::shared_ptr<SegmentData>> segments,
												   const std::string &sender);

  [[maybe_unused]] [[nodiscard]] const std::unordered_map<std::shared_ptr<SegmentKey>, std::shared_ptr<SegmentData>> &getSegments() const;

  [[nodiscard]] std::string toString() const;

private:
  std::unordered_map<std::shared_ptr<SegmentKey>, std::shared_ptr<SegmentData>> segments_;

};

}

#endif //NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_CACHE_LOADRESPONSEMESSAGE_H
