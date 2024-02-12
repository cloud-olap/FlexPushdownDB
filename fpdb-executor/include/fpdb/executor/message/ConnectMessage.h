//
// Created by matt on 30/9/20.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_MESSAGE_CONNECTMESSAGE_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_MESSAGE_CONNECTMESSAGE_H

#include <fpdb/executor/message/Message.h>
#include <fpdb/executor/physical/POpConnection.h>
#include <vector>

using namespace fpdb::executor::physical;

namespace fpdb::executor::message {

/**
 * Message sent to operators to tell them who they are connected to
 */
class ConnectMessage : public Message {

public:
  explicit ConnectMessage(std::vector<POpConnection> operatorConnections, std::string from);
  ConnectMessage() = default;
  ConnectMessage(const ConnectMessage&) = default;
  ConnectMessage& operator=(const ConnectMessage&) = default;

  std::string getTypeString() const override;

  const std::vector<POpConnection> &connections() const;

private:
  std::vector<POpConnection> connections_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, ConnectMessage& msg) {
    return f.object(msg).fields(f.field("type", msg.type_),
                                f.field("sender", msg.sender_),
                                f.field("connections", msg.connections_));
  }
};

}

#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_MESSAGE_CONNECTMESSAGE_H
