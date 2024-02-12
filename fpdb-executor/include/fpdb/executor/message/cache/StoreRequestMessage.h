//
// Created by matt on 20/5/20.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_MESSAGE_CACHE_STOREREQUESTMESSAGE_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_MESSAGE_CACHE_STOREREQUESTMESSAGE_H

#include <fpdb/executor/message/Message.h>
#include <fpdb/cache/SegmentKey.h>
#include <fpdb/cache/SegmentData.h>
#include <fpdb/caf/CAFUtil.h>

using namespace fpdb::cache;

namespace fpdb::executor::message {

/**
 * Reuest for the segment cache actor to store the given segment data given a segment key.
 */
class StoreRequestMessage : public Message {

public:
  StoreRequestMessage(std::unordered_map<std::shared_ptr<SegmentKey>, std::shared_ptr<SegmentData>> segments,
					  const std::string &sender);
  StoreRequestMessage() = default;
  StoreRequestMessage(const StoreRequestMessage&) = default;
  StoreRequestMessage& operator=(const StoreRequestMessage&) = default;

  static std::shared_ptr<StoreRequestMessage>
  make(const std::unordered_map<std::shared_ptr<SegmentKey>, std::shared_ptr<SegmentData>>& segments,
	   const std::string &sender);

  std::string getTypeString() const override;

  [[nodiscard]] const std::unordered_map<std::shared_ptr<SegmentKey>, std::shared_ptr<SegmentData>> &
  getSegments() const;

  [[nodiscard]] std::string toString() const;

private:
  std::unordered_map<std::shared_ptr<SegmentKey>, std::shared_ptr<SegmentData>> segments_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, StoreRequestMessage& msg) {
    return f.object(msg).fields(f.field("type", msg.type_),
                                f.field("sender", msg.sender_),
                                f.field("segments", msg.segments_));
  };
};

}

using StoreRequestMessagePtr = std::shared_ptr<fpdb::executor::message::StoreRequestMessage>;

namespace caf {
template <>
struct inspector_access<StoreRequestMessagePtr> : variant_inspector_access<StoreRequestMessagePtr> {
  // nop
};
} // namespace caf

#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_MESSAGE_CACHE_STOREREQUESTMESSAGE_H
