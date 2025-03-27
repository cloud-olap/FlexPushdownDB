//
// Created by Yifei Yang on 10/24/23.
//

#include <fpdb/executor/message/GlobalArrowBloomFilterInitMessage.h>

namespace fpdb::executor::message {

GlobalArrowBloomFilterInitMessage::GlobalArrowBloomFilterInitMessage(
        int threadId,
        const std::shared_ptr<executor::physical::bloomfilter::GlobalArrowBloomFilter> &bloomFilter,
        const std::string &sender):
  Message(GLOBAL_BLOOM_FILTER_INIT, sender),
  threadId_(threadId),
  bloomFilter_(bloomFilter) {}

std::string GlobalArrowBloomFilterInitMessage::getTypeString() const {
  return "GlobalArrowBloomFilterInitMessage";
}

int GlobalArrowBloomFilterInitMessage::getThreadId() const {
  return threadId_;
}

const std::shared_ptr<executor::physical::bloomfilter::GlobalArrowBloomFilter>
&GlobalArrowBloomFilterInitMessage::getBloomFilter() const {
  return bloomFilter_;
}

}