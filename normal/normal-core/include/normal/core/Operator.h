//
// Created by matt on 5/12/19.
//

#ifndef NORMAL_NORMAL_CORE_SRC_OPERATOR_H
#define NORMAL_NORMAL_CORE_SRC_OPERATOR_H

#include <string>
#include <memory>
#include <map>

#include <caf/all.hpp>

#include "normal/core/message/Message.h"
#include "normal/core/OperatorContext.h"
#include "normal/core/message/Envelope.h"
#include "Globals.h"
#include <normal/core/Forward.h>

namespace normal::core {

/**
 * Base class for operators
 */
class Operator {

private:
  std::string name_;
  std::string type_;
  std::shared_ptr<OperatorContext> opContext_;
  std::map<std::string, std::string> producers_;
  std::map<std::string, std::string> consumers_;

public:
  explicit Operator(std::string name, std::string type);
  virtual ~Operator() = default;

  std::string &name();
  std::shared_ptr<OperatorContext> ctx();
  [[ deprecated("Use std::shared_ptr<OperatorContext> ctx()") ]]
  std::shared_ptr<OperatorContext> weakCtx();
  void setName(const std::string &Name);
  virtual void onReceive(const normal::core::message::Envelope &msg) = 0;

  std::map<std::string, std::string> producers();
  std::map<std::string, std::string> consumers();

  void create(const std::shared_ptr<OperatorContext>& ctx);
  virtual void produce(const std::shared_ptr<Operator> &operator_);
  virtual void consume(const std::shared_ptr<Operator> &operator_);
  const std::string &getType() const;

  void destroyActor();

};

} // namespace

#endif //NORMAL_NORMAL_CORE_SRC_OPERATOR_H
