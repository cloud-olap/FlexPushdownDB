//
// Created by Yifei Yang on 7/3/22.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_MESSAGE_TUPLESETREADYREMOTEMESSAGE_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_MESSAGE_TUPLESETREADYREMOTEMESSAGE_H

#include <fpdb/executor/message/Message.h>
#include <memory>

namespace fpdb::executor::message {

/**
 * Message denoting the input tupleSet is ready at a remote node
 */
class TupleSetReadyRemoteMessage : public Message {

public:
  explicit TupleSetReadyRemoteMessage(const std::string &host,
                                      int port,
                                      bool isFromStore,
                                      const std::string &sender);
  TupleSetReadyRemoteMessage() = default;
  TupleSetReadyRemoteMessage(const TupleSetReadyRemoteMessage&) = default;
  TupleSetReadyRemoteMessage& operator=(const TupleSetReadyRemoteMessage&) = default;
  ~TupleSetReadyRemoteMessage() override = default;

  std::string getTypeString() const override;

  const std::string &getHost() const;
  int getPort() const;
  bool isFromStore() const;

private:
  std::string host_;
  int port_;
  bool isFromStore_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, TupleSetReadyRemoteMessage& msg) {
    return f.object(msg).fields(f.field("type", msg.type_),
                                f.field("sender", msg.sender_),
                                f.field("host", msg.host_),
                                f.field("port", msg.port_),
                                f.field("isFromStore", msg.isFromStore_));
  }
};

}


#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_MESSAGE_TUPLESETREADYREMOTEMESSAGE_H
