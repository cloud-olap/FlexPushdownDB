//
// Created by Yifei Yang on 9/9/20.
//

#ifndef NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_CACHE_SEGMENTWEIGHTMESSAGE_H
#define NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_CACHE_SEGMENTWEIGHTMESSAGE_H

#include <normal/core/message/Message.h>
#include <normal/cache/SegmentKey.h>
#include <vector>

using namespace normal::cache;
using namespace normal::core::message;

namespace normal::core::cache {

/**
 * A message to cache of segment metadata
 */
class WeightRequestMessage : public Message {

public:
  WeightRequestMessage(const std::shared_ptr<std::vector<std::shared_ptr<SegmentKey>>> &segmentKeys,
                       double weight,
                       long queryId,
                       const std::string &sender);

  static std::shared_ptr<WeightRequestMessage> make(const std::shared_ptr<std::vector<std::shared_ptr<SegmentKey>>> &segmentKeys,
                                                    double weight,
                                                    long queryId,
                                                    const std::string &sender);

  const std::shared_ptr<std::vector<std::shared_ptr<SegmentKey>>> &getSegmentKeys() const;
  double getWeight() const;
  long getQueryId() const;

private:
  std::shared_ptr<std::vector<std::shared_ptr<SegmentKey>>> segmentKeys_;
  double weight_;
  long queryId_;
};

}


#endif //NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_CACHE_SEGMENTWEIGHTMESSAGE_H
