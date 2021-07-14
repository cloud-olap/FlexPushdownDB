//
// Created by matt on 6/8/20.
//

#ifndef NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_BLOOMJOIN_BLOOMFILTERMESSAGE_H
#define NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_BLOOMJOIN_BLOOMFILTERMESSAGE_H

#include <memory>
#include <utility>

#include <normal/core/message/Message.h>

#include "SlicedBloomFilter.h"

using namespace normal::core::message;

class BloomFilterMessage : public Message {

public:
  BloomFilterMessage(std::shared_ptr<SlicedBloomFilter> bloomFilter, const std::string &sender) :
	  Message("BloomFilterMessage", sender), bloomFilter_(std::move(bloomFilter)) {
  }

  [[nodiscard]] const std::shared_ptr<SlicedBloomFilter> &getBloomFilter() const {
	return bloomFilter_;
  }

private:
  std::shared_ptr<SlicedBloomFilter> bloomFilter_;

};

#endif //NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_BLOOMJOIN_BLOOMFILTERMESSAGE_H
