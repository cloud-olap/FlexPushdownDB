//
// Created by Yifei Yang on 11/2/20.
//

#ifndef NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_CACHE_CACHEMETRICSMESSAGE_H
#define NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_CACHE_CACHEMETRICSMESSAGE_H

#include <normal/core/message/Message.h>
#include <memory>

using namespace normal::core::message;

namespace normal::core::cache {

class CacheMetricsMessage : public Message {

public:
  CacheMetricsMessage(size_t hitNum, size_t missNum, size_t shardHitNum, size_t shardMissNum, const std::string &sender);
  static std::shared_ptr<CacheMetricsMessage> make(size_t hitNum, size_t missNum, size_t shardHitNum, size_t shardMissNum, const std::string &sender);

  size_t getHitNum() const;
  size_t getMissNum() const;

  size_t getShardHitNum() const;
  size_t getShardMissNum() const;

private:
  size_t hitNum_;
  size_t missNum_;
  size_t shardHitNum_;
  size_t shardMissNum_;
};

}


#endif //NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_CACHE_CACHEMETRICSMESSAGE_H
