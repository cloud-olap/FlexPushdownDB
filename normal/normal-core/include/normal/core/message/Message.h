//
// Created by matt on 9/12/19.
//

#ifndef NORMAL_NORMAL_NORMAL_CORE_SRC_MESSAGE_H
#define NORMAL_NORMAL_NORMAL_CORE_SRC_MESSAGE_H

#include <string>

namespace normal::core::message {

/**
 * Base class for messages
 */
class Message {

private:
  std::string type_;
  std::string sender_;

public:
  explicit Message(std::string type, std::string sender);
  virtual ~Message() = 0;
  [[nodiscard]] std::string type() const;
  [[nodiscard]] std::string sender() const;

};

} // namespace

#endif //NORMAL_NORMAL_NORMAL_CORE_SRC_MESSAGE_H
