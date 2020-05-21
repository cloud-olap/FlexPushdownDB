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
  LoadResponseMessage(std::shared_ptr<SegmentKey> segmentKey,
					  std::optional<std::shared_ptr<SegmentData>> segmentData,
					  const std::string &sender);

  static std::shared_ptr<LoadResponseMessage> make(std::shared_ptr<SegmentKey> segmentKey,
												   std::optional<std::shared_ptr<SegmentData>> segmentData,
												   const std::string &sender);

  [[maybe_unused]] [[nodiscard]] const std::shared_ptr<SegmentKey> &getSegmentKey() const;
  [[maybe_unused]] [[nodiscard]] const std::optional<std::shared_ptr<SegmentData>> &getSegmentData() const;

  [[nodiscard]] std::string toString() const;

private:
  std::shared_ptr<SegmentKey> segmentKey_;
  std::optional<std::shared_ptr<SegmentData>> segmentData_;

};

}

#endif //NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_CACHE_LOADRESPONSEMESSAGE_H
