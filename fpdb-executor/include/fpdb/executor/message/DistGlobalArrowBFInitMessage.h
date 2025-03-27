//
// Created by Yifei Yang on 4/17/24.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_MESSAGE_DISTGLOBALARROWBFINITMESSAGE_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_MESSAGE_DISTGLOBALARROWBFINITMESSAGE_H

#include <fpdb/executor/message/Message.h>
#include <fpdb/tuple/arrow/exec/BloomFilter.h>

namespace fpdb::executor::message {

/**
 * Message sent to all nodes to start build a single distributed global bloom filter in the cluster level,
 * each node will build a copy and send it to the same node to merge.
 */
class DistGlobalArrowBFInitMessage: public Message {
public:
  DistGlobalArrowBFInitMessage(int64_t numRows,
                               const std::shared_ptr<arrow::compute::BloomFilterMasks> &masks,
                               const std::string &sender);
  DistGlobalArrowBFInitMessage() = default;
  DistGlobalArrowBFInitMessage(const DistGlobalArrowBFInitMessage&) = default;
  DistGlobalArrowBFInitMessage& operator=(const DistGlobalArrowBFInitMessage&) = default;
  ~DistGlobalArrowBFInitMessage() = default;

  std::string getTypeString() const override;

  int64_t getNumRows() const;
  const std::shared_ptr<arrow::compute::BloomFilterMasks> &getMasks() const;

private:
  int64_t numRows_;
  std::shared_ptr<arrow::compute::BloomFilterMasks> masks_;   // since masks are small (100B), we just send by actors
                                                              // instead of using Flight for simplicity

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, DistGlobalArrowBFInitMessage& msg) {
    return f.object(msg).fields(f.field("type", msg.type_),
                                f.field("sender", msg.sender_),
                                f.field("numRows", msg.numRows_),
                                f.field("masks", msg.masks_));
  }
};

}

#endif // FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_MESSAGE_DISTGLOBALARROWBFINITMESSAGE_H
