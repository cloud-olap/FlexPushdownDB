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

namespace normal::core {

class OperatorContext;

/**
 * Base class for operators
 */
class Operator {

private:
  std::string name_;
  std::string type_;
  long queryId_;
  std::weak_ptr<OperatorContext> opContext_;
  std::map<std::string, std::weak_ptr<Operator>> producers_;
  std::map<std::string, std::weak_ptr<Operator>> consumers_;
  caf::actor actorHandle_;

public:
  explicit Operator(std::string name, std::string type, long queryId);
  virtual ~Operator() = default;

  std::string &name();
  [[nodiscard]] const caf::actor& actorHandle() const;
  void actorHandle(caf::actor actorId);
  [[ deprecated("Use std::weak_ptr<OperatorContext> weakCtx()") ]]
  std::shared_ptr<OperatorContext> ctx();
  std::weak_ptr<OperatorContext> weakCtx();
  void setName(const std::string &Name);
  virtual void onReceive(const normal::core::message::Envelope &msg) = 0;
  long getQueryId() const;

  std::map<std::string, std::weak_ptr<Operator>> producers();
  std::map<std::string, std::weak_ptr<Operator>> consumers();

  void create(const std::shared_ptr<OperatorContext>& ctx);
  virtual void produce(const std::shared_ptr<Operator> &operator_);
  virtual void consume(const std::shared_ptr<Operator> &operator_);
  const std::string &getType() const;

  void destroyActor();

};

} // namespace

#endif //NORMAL_NORMAL_CORE_SRC_OPERATOR_H
