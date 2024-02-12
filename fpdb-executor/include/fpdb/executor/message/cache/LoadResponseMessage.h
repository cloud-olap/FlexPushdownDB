//
// Created by matt on 20/5/20.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_MESSAGE_CACHE_LOADRESPONSEMESSAGE_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_MESSAGE_CACHE_LOADRESPONSEMESSAGE_H

#include <fpdb/executor/message/Message.h>
#include <fpdb/cache/SegmentKey.h>
#include <fpdb/cache/SegmentData.h>
#include <fpdb/caf/CAFUtil.h>
#include <unordered_map>

using namespace fpdb::cache;

namespace fpdb::executor::message {

/**
 * Response sent from segment cache actor containing a segment loaded from cache, segment data is nullopt if segment was not found
 * in cache.
 */
class LoadResponseMessage : public Message {

public:
  LoadResponseMessage(std::unordered_map<std::shared_ptr<SegmentKey>, std::shared_ptr<SegmentData>, SegmentKeyPointerHash, SegmentKeyPointerPredicate> segments,
					            const std::string &sender,
                      std::vector<std::shared_ptr<SegmentKey>> segmentKeysToCache);
  LoadResponseMessage() = default;
  LoadResponseMessage(const LoadResponseMessage&) = default;
  LoadResponseMessage& operator=(const LoadResponseMessage&) = default;

  static std::shared_ptr<LoadResponseMessage> make(std::unordered_map<std::shared_ptr<SegmentKey>, std::shared_ptr<SegmentData>, SegmentKeyPointerHash, SegmentKeyPointerPredicate> segments,
												                           const std::string &sender,
                                                   std::vector<std::shared_ptr<SegmentKey>> segmentKeysToCache);

  std::string getTypeString() const override;

  [[maybe_unused]] [[nodiscard]] const std::unordered_map<std::shared_ptr<SegmentKey>, std::shared_ptr<SegmentData>, SegmentKeyPointerHash, SegmentKeyPointerPredicate> &getSegments() const;
  [[nodiscard]] const std::vector<std::shared_ptr<SegmentKey>> &getSegmentKeysToCache() const;

  [[nodiscard]] std::string toString() const;

private:
  std::unordered_map<std::shared_ptr<SegmentKey>, std::shared_ptr<SegmentData>, SegmentKeyPointerHash, SegmentKeyPointerPredicate> segments_;
  std::vector<std::shared_ptr<SegmentKey>> segmentKeysToCache_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, LoadResponseMessage& msg) {
    return f.object(msg).fields(f.field("type", msg.type_),
                                f.field("sender", msg.sender_),
                                f.field("segments", msg.segments_),
                                f.field("segmentKeysToCache", msg.segmentKeysToCache_));
  };
};

}

using LoadResponseMessagePtr = std::shared_ptr<fpdb::executor::message::LoadResponseMessage>;

namespace caf {
template <>
struct inspector_access<LoadResponseMessagePtr> : variant_inspector_access<LoadResponseMessagePtr> {
  // nop
};
} // namespace caf

#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_MESSAGE_CACHE_LOADRESPONSEMESSAGE_H
