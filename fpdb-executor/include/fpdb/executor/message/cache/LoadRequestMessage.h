//
// Created by matt on 20/5/20.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_MESSAGE_CACHE_LOADREQUESTMESSAGE_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_MESSAGE_CACHE_LOADREQUESTMESSAGE_H

#include <fpdb/executor/message/Message.h>
#include <fpdb/cache/SegmentKey.h>
#include <fpdb/cache/SegmentData.h>
#include <fpdb/caf/CAFUtil.h>

using namespace fpdb::cache;

namespace fpdb::executor::message {

/**
 * Request for the segment cache actor to load a segment given a segment key. On a hit, the segment is sent to the
 * requesting actor, or nullopt on a cache miss.
 */
class LoadRequestMessage : public Message {

public:
  LoadRequestMessage(std::vector<std::shared_ptr<SegmentKey>> segmentKeys,
							  const std::string &sender);
  LoadRequestMessage() = default;
  LoadRequestMessage(const LoadRequestMessage&) = default;
  LoadRequestMessage& operator=(const LoadRequestMessage&) = default;

  static std::shared_ptr<LoadRequestMessage> make(const std::vector<std::shared_ptr<SegmentKey>>& segmentKeys, const std::string &sender);

  std::string getTypeString() const override;

  [[nodiscard]] const std::vector<std::shared_ptr<SegmentKey>> &getSegmentKeys() const;

  [[nodiscard]] std::string toString() const;

private:
  std::vector<std::shared_ptr<SegmentKey>> segmentKeys_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, LoadRequestMessage& msg) {
    return f.object(msg).fields(f.field("type", msg.type_),
                                f.field("sender", msg.sender_),
                                f.field("segmentKeys", msg.segmentKeys_));
  }
};

}

using LoadRequestMessagePtr = std::shared_ptr<fpdb::executor::message::LoadRequestMessage>;

namespace caf {
template <>
struct inspector_access<LoadRequestMessagePtr> : variant_inspector_access<LoadRequestMessagePtr> {
  // nop
};
} // namespace caf

#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_MESSAGE_CACHE_LOADREQUESTMESSAGE_H
