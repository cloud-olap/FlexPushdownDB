//
// Created by matt on 5/12/19.
//

#ifndef NORMAL_NORMAL_CORE_SRC_OPERATOR_H
#define NORMAL_NORMAL_CORE_SRC_OPERATOR_H

#include <string>
#include <memory>
#include <map>

#include <caf/all.hpp>

#include "Message.h"
#include "OperatorContext.h"
#include "Envelope.h"

namespace normal::core {

//class Message;
class OperatorContext;

/**
 * Base class for operators
 */
class Operator {

private:
  std::string name_;
  std::shared_ptr<normal::core::OperatorContext> opContext_;
  std::map<std::string, std::shared_ptr<normal::core::Operator>> producers_;
  std::map<std::string, std::shared_ptr<normal::core::Operator>> consumers_;
  caf::actor actorHandle_;

public:
  explicit Operator(std::string name);
  virtual ~Operator() = 0;

  std::string &name();
  [[nodiscard]] caf::actor actorHandle() const;
  void actorHandle(caf::actor actorId);
  std::shared_ptr<normal::core::OperatorContext> ctx();

  virtual void onReceive(const normal::core::Envelope &msg);

  std::map<std::string, std::shared_ptr<normal::core::Operator>> producers();
  std::map<std::string, std::shared_ptr<normal::core::Operator>> consumers();

  void create(std::shared_ptr<normal::core::OperatorContext> ctx);
  void produce(const std::shared_ptr<normal::core::Operator> &op);
  void consume(const std::shared_ptr<normal::core::Operator> &op);

};

} // namespace

#endif //NORMAL_NORMAL_CORE_SRC_OPERATOR_H
