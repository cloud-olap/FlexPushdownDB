//
// Created by matt on 5/12/19.
//

#ifndef NORMAL_NORMAL_CORE_SRC_OPERATOR_H
#define NORMAL_NORMAL_CORE_SRC_OPERATOR_H

#include <string>
#include <vector>
#include <memory>

#include "OperatorContext.h"

class Message;

class Operator {
private:
  std::string m_name;
  bool m_created = false;
  bool m_running = false;
  std::shared_ptr<OperatorContext> m_operatorContext;
  std::vector<std::shared_ptr<Operator>> m_producers;
  std::vector<std::shared_ptr<Operator>> m_consumers;
protected:
  virtual void onStart() = 0;
  virtual void onStop() = 0;
  virtual void onReceive(std::unique_ptr<Message> msg);
public:

  explicit Operator(std::string name);
  virtual ~Operator() = 0;

  std::string &name();
  std::vector<std::shared_ptr<Operator>> consumers();
  std::shared_ptr<OperatorContext> ctx();

  void create(std::shared_ptr<OperatorContext> ctx);
  void start();
  void stop();
  void receive(std::unique_ptr<Message> msg);
  bool running();

  void produce(const std::shared_ptr<Operator> &op);
  void consume(const std::shared_ptr<Operator> &op);

};

#endif //NORMAL_NORMAL_CORE_SRC_OPERATOR_H
