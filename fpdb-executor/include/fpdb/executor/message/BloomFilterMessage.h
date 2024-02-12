//
// Created by Yifei Yang on 3/17/22.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_MESSAGE_BLOOMFILTERMESSAGE_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_MESSAGE_BLOOMFILTERMESSAGE_H

#include <fpdb/executor/message/Message.h>
#include <fpdb/executor/physical/bloomfilter/BloomFilterBase.h>

using namespace fpdb::executor::physical::bloomfilter;

namespace fpdb::executor::message {

/**
 * Message sent to BloomFilterPOp after BloomFilterCreatePOp creates the bloom filters
 */
class BloomFilterMessage: public Message {

public:
  explicit BloomFilterMessage(const std::shared_ptr<BloomFilterBase> &bloomFilter,
                              const std::string &sender);
  BloomFilterMessage() = default;
  BloomFilterMessage(const BloomFilterMessage&) = default;
  BloomFilterMessage& operator=(const BloomFilterMessage&) = default;
  ~BloomFilterMessage() = default;

  std::string getTypeString() const override;

  const std::shared_ptr<BloomFilterBase> &getBloomFilter() const;

private:
  std::shared_ptr<BloomFilterBase> bloomFilter_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, BloomFilterMessage& msg) {
    return f.object(msg).fields(f.field("type", msg.type_),
                                f.field("sender", msg.sender_),
                                f.field("bloomFilter", msg.bloomFilter_));
  }

};

}


#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_MESSAGE_BLOOMFILTERMESSAGE_H
