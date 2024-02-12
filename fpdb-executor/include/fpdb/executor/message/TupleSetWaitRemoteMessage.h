//
// Created by Yifei Yang on 10/10/22.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_MESSAGE_TUPLESETWAITREMOTEMESSAGE_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_MESSAGE_TUPLESETWAITREMOTEMESSAGE_H

#include <fpdb/executor/message/Message.h>
#include <memory>

namespace fpdb::executor::message {

/**
 * Message that lets the consumer wait at remote set to receive tables in pipeline.
 */
class TupleSetWaitRemoteMessage: public Message {

public:
  explicit TupleSetWaitRemoteMessage(const std::string &host, int port, const std::string &sender);
  TupleSetWaitRemoteMessage() = default;
  TupleSetWaitRemoteMessage(const TupleSetWaitRemoteMessage&) = default;
  TupleSetWaitRemoteMessage& operator=(const TupleSetWaitRemoteMessage&) = default;
  ~TupleSetWaitRemoteMessage() override = default;

  std::string getTypeString() const override;

  const std::string &getHost() const;
  int getPort() const;

private:
  std::string host_;
  int port_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, TupleSetWaitRemoteMessage& msg) {
    return f.object(msg).fields(f.field("type", msg.type_),
                                f.field("sender", msg.sender_),
                                f.field("host", msg.host_),
                                f.field("port", msg.port_));
  }
};

}


#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_MESSAGE_TUPLESETWAITREMOTEMESSAGE_H
