//
// Created by matt on 5/12/19.
//

#ifndef NORMAL_NORMAL_S3_SRC_COLLATE_H
#define NORMAL_NORMAL_S3_SRC_COLLATE_H

#include <string>
#include <memory>                  // for unique_ptr

#include <arrow/api.h>
#include <normal/core/TupleMessage.h>
#include <normal/core/CompleteMessage.h>

#include "normal/core/Operator.h"
#include "normal/core/OperatorContext.h"
#include "normal/core/TupleSet.h"

class Message;
class TupleSet;

namespace normal::pushdown {

class Collate : public normal::core::Operator {

private:
  std::shared_ptr<normal::core::TupleSet> m_tupleSet;
  void onStart();

  void onComplete(const normal::core::CompleteMessage &message);
  void onTuple(normal::core::TupleMessage Message);
  void onReceive(const normal::core::Envelope &msg) override;

public:
  explicit Collate(std::string name);
  ~Collate() override = default;
  void show();
  std::shared_ptr<normal::core::TupleSet> tuples();

};

}

#endif //NORMAL_NORMAL_S3_SRC_COLLATE_H
