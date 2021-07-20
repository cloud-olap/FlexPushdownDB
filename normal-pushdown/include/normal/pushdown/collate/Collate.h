//
// Created by matt on 5/12/19.
//

#ifndef NORMAL_NORMAL_S3_SRC_COLLATE_H
#define NORMAL_NORMAL_S3_SRC_COLLATE_H

#include <string>
#include <memory>                  // for unique_ptr

#include <arrow/api.h>
#include <normal/core/message/TupleMessage.h>
#include <normal/core/message/CompleteMessage.h>

#include "normal/core/Operator.h"
#include "normal/core/OperatorContext.h"
#include "normal/tuple/TupleSet.h"

namespace normal::tuple {
	class TupleSet;
}

namespace normal::pushdown::collate {

class Collate : public normal::core::Operator {

private:
  std::shared_ptr<TupleSet> tuples_;
private:
  std::vector<std::shared_ptr<arrow::Table>> tables_;
  size_t tablesCutoff_ = 20;
  void onStart();

  void onComplete(const normal::core::message::CompleteMessage &message);
  void onTuple(const normal::core::message::TupleMessage& message);
  void onReceive(const normal::core::message::Envelope &message) override;

public:
  explicit Collate(std::string name, long queryId = 0);
  ~Collate() override = default;
  void show();
  std::shared_ptr<TupleSet> tuples();
  [[maybe_unused]] void setTuples(const std::shared_ptr<TupleSet> &Tuples);
};

}

#endif //NORMAL_NORMAL_S3_SRC_COLLATE_H
