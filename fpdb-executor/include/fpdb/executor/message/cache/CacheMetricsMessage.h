//
// Created by Yifei Yang on 11/2/20.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_MESSAGE_CACHE_CACHEMETRICSMESSAGE_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_MESSAGE_CACHE_CACHEMETRICSMESSAGE_H

#include <fpdb/executor/message/Message.h>
#include <fpdb/caf/CAFUtil.h>
#include <memory>

namespace fpdb::executor::message {

class CacheMetricsMessage : public Message {

public:
  CacheMetricsMessage(size_t hitNum, size_t missNum, size_t shardHitNum, size_t shardMissNum, const std::string &sender);
  CacheMetricsMessage() = default;
  CacheMetricsMessage(const CacheMetricsMessage&) = default;
  CacheMetricsMessage& operator=(const CacheMetricsMessage&) = default;

  static std::shared_ptr<CacheMetricsMessage> make(size_t hitNum, size_t missNum, size_t shardHitNum, size_t shardMissNum, const std::string &sender);

  std::string getTypeString() const override;

  size_t getHitNum() const;
  size_t getMissNum() const;

  size_t getShardHitNum() const;
  size_t getShardMissNum() const;

private:
  size_t hitNum_;
  size_t missNum_;
  size_t shardHitNum_;
  size_t shardMissNum_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, CacheMetricsMessage& msg) {
    return f.object(msg).fields(f.field("type", msg.type_),
                                f.field("sender", msg.sender_),
                                f.field("hitNum", msg.hitNum_),
                                f.field("missNum", msg.missNum_),
                                f.field("shardHitNum", msg.shardHitNum_),
                                f.field("shardMissNum", msg.shardMissNum_));
  };
};

}

using CacheMetricsMessagePtr = std::shared_ptr<fpdb::executor::message::CacheMetricsMessage>;

namespace caf {
template <>
struct inspector_access<CacheMetricsMessagePtr> : variant_inspector_access<CacheMetricsMessagePtr> {
  // nop
};
} // namespace caf

#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_MESSAGE_CACHE_CACHEMETRICSMESSAGE_H
