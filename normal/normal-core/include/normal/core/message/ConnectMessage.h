//
// Created by matt on 30/9/20.
//

#ifndef NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_MESSAGE_CONNECTMESSAGE_H
#define NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_MESSAGE_CONNECTMESSAGE_H

#include <vector>

#include <normal/core/OperatorConnection.h>

#include "normal/core/message/Message.h"

namespace normal::core::message {

/**
 * Message sent to operators to tell them who they are connected to
 */
class ConnectMessage : public Message {

private:
  std::vector<OperatorConnection> operatorConnections_;

public:
  explicit ConnectMessage(std::vector<OperatorConnection> operatorConnections, std::string from);
  [[nodiscard]] const std::vector<OperatorConnection> &connections() const;

};

}

#endif //NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_MESSAGE_CONNECTMESSAGE_H
