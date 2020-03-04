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
  bool m_created = false;
  bool m_running = false;
  std::shared_ptr<normal::core::OperatorContext> m_operatorContext;
  std::map<std::string, std::shared_ptr<normal::core::Operator>> m_producers;
  std::map<std::string, std::shared_ptr<normal::core::Operator>> m_consumers;
  caf::actor_id actorId;
public:
  [[nodiscard]] caf::actor_id getActorId() const;
  void setActorId(caf::actor_id actorId);

protected:
  virtual void onStart() = 0;
  virtual void onStop() = 0;
  virtual void onReceive(const  normal::core::Envelope &msg);
  virtual void onComplete(const Operator &op);

public:

  explicit Operator(std::string name);
  virtual ~Operator() = 0;

  std::string &name();
  std::map<std::string, std::shared_ptr<normal::core::Operator>> producers();
  std::map<std::string, std::shared_ptr<normal::core::Operator>> consumers();
  std::shared_ptr<normal::core::OperatorContext> ctx();

  void create(std::shared_ptr<normal::core::OperatorContext> ctx);
  void start();
  void stop();
  void receive(const normal::core::Envelope &msg);
  void complete(const normal::core::Operator &consumer);
  bool isRunning();
  void produce(const std::shared_ptr<normal::core::Operator> &op);
  void consume(const std::shared_ptr<normal::core::Operator> &op);
};

} // namespace

#endif //NORMAL_NORMAL_CORE_SRC_OPERATOR_H
