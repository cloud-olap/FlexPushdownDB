//
// Created by matt on 5/12/19.
//

#ifndef NORMAL_NORMAL_CORE_SRC_OPERATOR_H
#define NORMAL_NORMAL_CORE_SRC_OPERATOR_H

#include <string>
#include <vector>
#include <memory>
#include <map>
#include <caf/all.hpp>
#include "OperatorContext.h"

class Message;
class OperatorContext;

namespace normal::core {

/**
 * Base class for operators
 */
class Operator {

private:
  std::string m_name;
  bool m_created = false;
  bool m_running = false;
  std::shared_ptr<OperatorContext> m_operatorContext;
  std::map<std::string, std::shared_ptr<Operator>> m_producers;
  std::map<std::string, std::shared_ptr<Operator>> m_consumers;
  caf::actor_id actorId;
public:
  caf::actor_id getActorId() const;
  void setActorId(caf::actor_id actorId);

protected:
  virtual void onStart() = 0;
  virtual void onStop() = 0;
  virtual void onReceive(const Message &msg);
  virtual void onComplete(const Operator &op);

public:

  explicit Operator(std::string name);
  virtual ~Operator() = 0;

  std::string &name();
  std::map<std::string, std::shared_ptr<Operator>> producers();
  std::map<std::string, std::shared_ptr<Operator>> consumers();
  std::shared_ptr<OperatorContext> ctx();

  void create(std::shared_ptr<OperatorContext> ctx);
  void start();
  void stop();
  void receive(const Message &msg);
  void complete(const Operator &consumer);
  bool isRunning();
  void produce(const std::shared_ptr<Operator> &op);
  void consume(const std::shared_ptr<Operator> &op);
};

} // namespace

#endif //NORMAL_NORMAL_CORE_SRC_OPERATOR_H
