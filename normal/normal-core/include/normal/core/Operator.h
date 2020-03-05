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
  std::string m_name;
  std::shared_ptr<normal::core::OperatorContext> m_operatorContext;
  std::map<std::string, std::shared_ptr<normal::core::Operator>> m_producers;
  std::map<std::string, std::shared_ptr<normal::core::Operator>> m_consumers;
  caf::actor actorHandle_;

public:
  explicit Operator(std::string name);
  virtual ~Operator() = 0;

  [[nodiscard]] caf::actor actorHandle() const;
  void actorHandle(caf::actor actorId);

  virtual void onReceive(const normal::core::Envelope &msg);

  std::string &name();
  std::map<std::string, std::shared_ptr<normal::core::Operator>> producers();
  std::map<std::string, std::shared_ptr<normal::core::Operator>> consumers();
  std::shared_ptr<normal::core::OperatorContext> ctx();
  void create(std::shared_ptr<normal::core::OperatorContext> ctx);
  void produce(const std::shared_ptr<normal::core::Operator> &op);
  void consume(const std::shared_ptr<normal::core::Operator> &op);
};

} // namespace

#endif //NORMAL_NORMAL_CORE_SRC_OPERATOR_H
