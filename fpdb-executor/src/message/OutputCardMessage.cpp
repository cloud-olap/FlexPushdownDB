//
// Created by Yifei Yang on 4/15/24.
//

#include <fpdb/executor/message/OutputCardMessage.h>

namespace fpdb::executor::message {

OutputCardMessage::OutputCardMessage(const cache::OutputCardCache::OutputCardKey& key,
                                     int64_t card,
                                     const std::string& sender):
  Message(OUTPUT_CARD, sender), key_(key), card_(card) {
}

std::string OutputCardMessage::getTypeString() const {
  return "OutputCardMessage";
}

const cache::OutputCardCache::OutputCardKey& OutputCardMessage::getKey() const {
  return key_;
}

int64_t OutputCardMessage::getCard() const {
  return card_;
}

}
