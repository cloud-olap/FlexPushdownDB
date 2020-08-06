//
// Created by matt on 5/8/20.
//

#ifndef NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_BLOOMJOIN_FILESCANBLOOMUSEOPERATOR_H
#define NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_BLOOMJOIN_FILESCANBLOOMUSEOPERATOR_H

#include <normal/core/Operator.h>
#include <normal/core/message/CompleteMessage.h>
#include <normal/pushdown/TupleMessage.h>

#include "FileScanBloomUseKernel.h"

using namespace normal::core;
using namespace normal::core::message;

namespace normal::pushdown::bloomjoin {

class FileScanBloomUseOperator : public Operator {

public:
  FileScanBloomUseOperator(const std::string &name, std::string  columnName);

  void onReceive(const Envelope &msg) override;

private:

  std::string columnName_;

  FileScanBloomUseKernel kernel_;

  void onStart();
  void onTuple(const TupleMessage &msg);
  void onComplete(const CompleteMessage &msg);

};

}

#endif //NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_BLOOMJOIN_FILESCANBLOOMUSEOPERATOR_H
