//
// Created by matt on 20/5/20.
//

#ifndef NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_CACHE_STOREREQUESTMESSAGE_H
#define NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_CACHE_STOREREQUESTMESSAGE_H

#include <caf/all.hpp>
#include <normal/cache/SegmentKey.h>
#include <normal/cache/SegmentData.h>

using namespace caf;
using namespace normal::cache;

namespace normal::core::cache {

/**
 * Reuest for the segment cache actor to store the given segment data given a segment key.
 */
class StoreRequestMessage {

public:
  StoreRequestMessage(std::shared_ptr<SegmentKey> SegmentKey, std::shared_ptr<SegmentData> SegmentData);

  static std::shared_ptr<StoreRequestMessage> make(const std::shared_ptr<SegmentKey> &SegmentKey, const std::shared_ptr<SegmentData> &SegmentData);

  [[nodiscard]] const std::shared_ptr<SegmentKey> &getSegmentKey() const;
  [[nodiscard]] const std::shared_ptr<SegmentData> &getSegmentData() const;

  std::string toString();

private:
  std::shared_ptr<SegmentKey> segmentKey_;
  std::shared_ptr<SegmentData> segmentData_;

};

}

CAF_ALLOW_UNSAFE_MESSAGE_TYPE(normal::core::cache::StoreRequestMessage)

#endif //NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_CACHE_STOREREQUESTMESSAGE_H
