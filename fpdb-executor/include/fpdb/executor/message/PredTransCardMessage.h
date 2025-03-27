//
// Created by Yifei Yang on 4/11/24.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_MESSAGE_PREDTRANSCARDMESSAGE_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_MESSAGE_PREDTRANSCARDMESSAGE_H

#include <fpdb/executor/message/Message.h>
#include <fpdb/executor/cache/CardCache.h>
#include <memory>

namespace fpdb::executor::message {

class PredTransCardMessage: public Message {
  
public:
  PredTransCardMessage(const cache::PredTransCardCache::PredTransCardKey &key,
                       const cache::PredTransCardCache::PredTransCardValue &value,
                       const std::string &sender);
  PredTransCardMessage() = default;
  PredTransCardMessage(const PredTransCardMessage&) = default;
  PredTransCardMessage& operator=(const PredTransCardMessage&) = default;
  ~PredTransCardMessage() override = default;
  
  std::string getTypeString() const override;

  const cache::PredTransCardCache::PredTransCardKey &getKey() const;
  const cache::PredTransCardCache::PredTransCardValue &getValue() const;

private:
  cache::PredTransCardCache::PredTransCardKey key_;
  cache::PredTransCardCache::PredTransCardValue value_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, PredTransCardMessage& msg) {
    return f.object(msg).fields(f.field("type", msg.type_),
                                f.field("sender", msg.sender_),
                                f.field("key", msg.key_),
                                f.field("value", msg.value_));
  }
};

}

#endif // FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_MESSAGE_PREDTRANSCARDMESSAGE_H
