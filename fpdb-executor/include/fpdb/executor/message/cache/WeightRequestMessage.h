//
// Created by Yifei Yang on 9/9/20.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_MESSAGE_CACHE_WEIGHTREQUESTMESSAGE_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_MESSAGE_CACHE_WEIGHTREQUESTMESSAGE_H

#include <fpdb/executor/message/Message.h>
#include <fpdb/cache/SegmentKey.h>
#include <fpdb/caf/CAFUtil.h>
#include <unordered_map>

using namespace fpdb::cache;

namespace fpdb::executor::message {

/**
 * A message to update segment weights
 */
class WeightRequestMessage : public Message {

public:
  WeightRequestMessage(const std::unordered_map<std::shared_ptr<SegmentKey>, double> &weightMap,
                       const std::string &sender);
  WeightRequestMessage() = default;
  WeightRequestMessage(const WeightRequestMessage&) = default;
  WeightRequestMessage& operator=(const WeightRequestMessage&) = default;

  static std::shared_ptr<WeightRequestMessage>
  make(const std::unordered_map<std::shared_ptr<SegmentKey>, double> &weightMap, const std::string &sender);

  std::string getTypeString() const override;

  const std::unordered_map<std::shared_ptr<SegmentKey>, double> &getWeightMap() const;

private:
  std::unordered_map<std::shared_ptr<SegmentKey>, double> weightMap_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, WeightRequestMessage& msg) {
    return f.object(msg).fields(f.field("type", msg.type_),
                                f.field("sender", msg.sender_),
                                f.field("weightMap", msg.weightMap_));
  };
};

}

using WeightRequestMessagePtr = std::shared_ptr<fpdb::executor::message::WeightRequestMessage>;

namespace caf {
template <>
struct inspector_access<WeightRequestMessagePtr> : variant_inspector_access<WeightRequestMessagePtr> {
  // nop
};
} // namespace caf

#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_MESSAGE_CACHE_WEIGHTREQUESTMESSAGE_H
