//
// Created by matt on 5/8/20.
//

#ifndef NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_BLOOMJOIN_BLOOMCREATEOPERATOR_H
#define NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_BLOOMJOIN_BLOOMCREATEOPERATOR_H

#include <normal/core/Operator.h>
#include <normal/core/message/CompleteMessage.h>
#include <normal/pushdown/TupleMessage.h>
#include "BloomCreateKernel.h"

using namespace normal::core;
using namespace normal::core::message;

namespace normal::pushdown::bloomjoin {

class BloomCreateOperator : public Operator {
public:
  explicit BloomCreateOperator(const std::string &name, std::string columnName);

  static std::shared_ptr<BloomCreateOperator> make(const std::string &name, const std::string &columnName);

  void onReceive(const Envelope &msg) override;

private:

  std::string columnName_;

  BloomCreateKernel kernel_;

  void onStart();
  void onTuple(const TupleMessage &msg);
  void onComplete(const CompleteMessage &msg);

};

}

#endif //NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_BLOOMJOIN_BLOOMCREATEOPERATOR_H
