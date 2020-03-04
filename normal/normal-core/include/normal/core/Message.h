//
// Created by matt on 9/12/19.
//

#ifndef NORMAL_NORMAL_NORMAL_CORE_SRC_MESSAGE_H
#define NORMAL_NORMAL_NORMAL_CORE_SRC_MESSAGE_H

#include <string>

namespace normal::core {

/**
 * Base class for messages
 *
 * FIXME: CAF doesn't appear to support abstract classes as messages
 */
class Message {
private:
  std::string type_;
public:
  [[nodiscard]] std::string type() const;
public:
//  explicit Message(std::string type);
  Message() = default;
  virtual ~Message() = 0;

};

} // namespace

#endif //NORMAL_NORMAL_NORMAL_CORE_SRC_MESSAGE_H
