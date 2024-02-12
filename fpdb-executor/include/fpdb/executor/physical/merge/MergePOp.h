//
// Created by matt on 20/7/20.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_MERGE_MERGEPOP_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_MERGE_MERGEPOP_H

#include <fpdb/executor/physical/PhysicalOp.h>
#include <fpdb/executor/message/CompleteMessage.h>
#include <fpdb/executor/message/TupleSetMessage.h>
#include <fpdb/tuple/TupleSet.h>
#include <queue>

using namespace fpdb::executor::message;

namespace fpdb::executor::physical::merge {

class MergePOp : public PhysicalOp {

public:
  explicit MergePOp(const std::string &name,
                    const std::vector<std::string> &projectColumnNames,
                    int nodeId);
  MergePOp() = default;
  MergePOp(const MergePOp&) = default;
  MergePOp& operator=(const MergePOp&) = default;

  void onReceive(const Envelope &msg) override;
  void clear() override;
  std::string getTypeString() const override;

  void setLeftProducer(const std::shared_ptr<PhysicalOp> &leftProducer);
  void setRightProducer(const std::shared_ptr<PhysicalOp> &rightProducer);

private:
  void onStart();
  void onComplete(const CompleteMessage &);
  void onTupleSet(const TupleSetMessage &message);

  void merge();

  std::string leftProducerName_;
  std::string rightProducerName_;

  std::list<std::shared_ptr<TupleSet>> leftTupleSets_;
  std::list<std::shared_ptr<TupleSet>> rightTupleSets_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, MergePOp& op) {
    return f.object(op).fields(f.field("name", op.name_),
                               f.field("type", op.type_),
                               f.field("projectColumnNames", op.projectColumnNames_),
                               f.field("nodeId", op.nodeId_),
                               f.field("queryId", op.queryId_),
                               f.field("opContext", op.opContext_),
                               f.field("producers", op.producers_),
                               f.field("consumers", op.consumers_),
                               f.field("consumerToBloomFilterInfo", op.consumerToBloomFilterInfo_),
                               f.field("isSeparated", op.isSeparated_),
                               f.field("leftProducerName", op.leftProducerName_),
                               f.field("rightProducerName", op.rightProducerName_));
  }
};

}

#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_MERGE_MERGEPOP_H
