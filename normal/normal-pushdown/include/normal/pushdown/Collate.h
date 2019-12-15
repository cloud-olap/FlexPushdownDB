//
// Created by matt on 5/12/19.
//

#ifndef NORMAL_NORMAL_S3_SRC_COLLATE_H
#define NORMAL_NORMAL_S3_SRC_COLLATE_H

#include <string>
#include <memory>                  // for unique_ptr

#include <arrow/api.h>

#include "normal/core/Operator.h"
#include "normal/core/OperatorContext.h"
#include "normal/core/TupleSet.h"

class Message;
class TupleSet;

class Collate : public Operator {
private:
  std::shared_ptr<TupleSet> m_tupleSet;
public:
  explicit Collate(std::string name);
  ~Collate() override = default;
  void onStart() override;
  void onStop() override;
  void onReceive(std::unique_ptr<Message> msg) override;
  void show();
protected:
  void onComplete(const Operator &op) override;

};

#endif //NORMAL_NORMAL_S3_SRC_COLLATE_H
