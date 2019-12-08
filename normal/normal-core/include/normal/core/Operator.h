//
// Created by matt on 5/12/19.
//

#ifndef NORMAL_NORMAL_CORE_SRC_OPERATOR_H
#define NORMAL_NORMAL_CORE_SRC_OPERATOR_H

#include "normal/core/OperatorContext.h"
#include <string>
#include <vector>
#include <memory>

class OperatorContext;

class Operator {
private:
  std::string m_name;
  bool m_running = false;
  std::shared_ptr<OperatorContext> m_operatorContext;
  std::vector<std::shared_ptr<Operator>> m_producers;
  std::vector<std::shared_ptr<Operator>> m_consumers;
public:

  explicit Operator(std::string name);

  std::string &name();
  std::vector<std::shared_ptr<Operator>> consumers();
  std::shared_ptr<OperatorContext> ctx();

  void start(std::shared_ptr<OperatorContext> ctx);
  void stop();
  bool running();

  void produce(const std::shared_ptr<Operator> &op);
  void consume(const std::shared_ptr<Operator> &op);

  virtual void onStart() = 0;
  virtual void onStop() = 0;
  virtual void onReceive(std::string msg) = 0;

};

#endif //NORMAL_NORMAL_CORE_SRC_OPERATOR_H
