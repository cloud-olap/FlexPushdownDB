//
// Created by Yifei Yang on 10/24/23.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_MESSAGE_GLOBALARROWBLOOMFILTERINITMESSAGE_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_MESSAGE_GLOBALARROWBLOOMFILTERINITMESSAGE_H

#include <fpdb/executor/message/Message.h>
#include <fpdb/executor/physical/bloomfilter/GlobalArrowBloomFilter.h>

namespace fpdb::executor::message {

/**
 * Message sent to multiple BloomFilterPOp to start build global bloom filters in parallel
 * for Arrow bloom filter only
 */
class GlobalArrowBloomFilterInitMessage: public Message {
public:
  explicit GlobalArrowBloomFilterInitMessage(
          int threadId,
          const std::shared_ptr<executor::physical::bloomfilter::GlobalArrowBloomFilter> &bloomFilter,
          const std::string &sender);
  GlobalArrowBloomFilterInitMessage() = default;
  GlobalArrowBloomFilterInitMessage(const GlobalArrowBloomFilterInitMessage&) = default;
  GlobalArrowBloomFilterInitMessage& operator=(const GlobalArrowBloomFilterInitMessage&) = default;
  ~GlobalArrowBloomFilterInitMessage() = default;

  std::string getTypeString() const override;

  int getThreadId() const;
  const std::shared_ptr<executor::physical::bloomfilter::GlobalArrowBloomFilter> &getBloomFilter() const;

private:
  int threadId_;
  std::shared_ptr<executor::physical::bloomfilter::GlobalArrowBloomFilter> bloomFilter_;

// caf inspect (never called since this message will only be transferred locally)
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, GlobalArrowBloomFilterInitMessage& msg) {
    return f.object(msg).fields(f.field("type", msg.type_),
                                f.field("sender", msg.sender_));
  }
};

}

#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_MESSAGE_GLOBALARROWBLOOMFILTERINITMESSAGE_H
