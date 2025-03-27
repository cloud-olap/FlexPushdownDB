//
// Created by Yifei Yang on 3/17/22.
//

#include <fpdb/executor/message/BloomFilterMessage.h>

namespace fpdb::executor::message {

BloomFilterMessage::BloomFilterMessage(const std::shared_ptr<BloomFilterBase> &bloomFilter,
                                       const std::string &sender):
  Message(BLOOM_FILTER, sender),
  bloomFilter_(bloomFilter) {}

std::string BloomFilterMessage::getTypeString() const {
  return "BloomFilterMessage";
}

const std::shared_ptr<BloomFilterBase> &BloomFilterMessage::getBloomFilter() const {
  return bloomFilter_;
}

const std::optional<RemoteInfo> &BloomFilterMessage::getRemoteInfo() const {
  return remoteInfo_;
}

bool BloomFilterMessage::isRemoteConsumerSpecific() const {
  return remoteConsumerSpecific_;
}

void BloomFilterMessage::setRemoteInfo(const RemoteInfo &remoteInfo) {
  remoteInfo_ = remoteInfo;
}

void BloomFilterMessage::setRemoteConsumerSpecific(bool remoteConsumerSpecific) {
  remoteConsumerSpecific_ = remoteConsumerSpecific;
}

}
