//
// Created by Yifei Yang on 4/11/24.
//

#include <fpdb/executor/message/PredTransCardMessage.h>

namespace fpdb::executor::message {

PredTransCardMessage::PredTransCardMessage(const cache::PredTransCardCache::PredTransCardKey &key,
                                           const cache::PredTransCardCache::PredTransCardValue &value,
                                           const std::string &sender):
  Message(PRED_TRANS_CARD, sender),
  key_(key), value_(value) {}

std::string PredTransCardMessage::getTypeString() const {
  return "PredTransCardMessage";
}

const cache::PredTransCardCache::PredTransCardKey &PredTransCardMessage::getKey() const {
  return key_;
}

const cache::PredTransCardCache::PredTransCardValue &PredTransCardMessage::getValue() const {
  return value_;
}

}
