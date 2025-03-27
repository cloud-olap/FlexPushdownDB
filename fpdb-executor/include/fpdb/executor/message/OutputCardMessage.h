//
// Created by Yifei Yang on 4/15/24.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_MESSAGE_OUTPUTCARDMESSAGE_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_MESSAGE_OUTPUTCARDMESSAGE_H

#include <fpdb/executor/message/Message.h>
#include <fpdb/executor/cache/CardCache.h>
#include <memory>

namespace fpdb::executor::message {

class OutputCardMessage: public Message {
  
public:
  OutputCardMessage(const cache::OutputCardCache::OutputCardKey &key,
                    int64_t card,
                    const std::string &sender);
  OutputCardMessage() = default;
  OutputCardMessage(const OutputCardMessage&) = default;
  OutputCardMessage& operator=(const OutputCardMessage&) = default;
  ~OutputCardMessage() override = default;

  std::string getTypeString() const override;

  const cache::OutputCardCache::OutputCardKey &getKey() const;
  int64_t getCard() const;

private:
  cache::OutputCardCache::OutputCardKey key_;
  int64_t card_;
  
// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, OutputCardMessage& msg) {
    return f.object(msg).fields(f.field("type", msg.type_),
                                f.field("sender", msg.sender_),
                                f.field("key", msg.key_),
                                f.field("card", msg.card_));
  }
};

}

#endif // FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_MESSAGE_OUTPUTCARDMESSAGE_H
