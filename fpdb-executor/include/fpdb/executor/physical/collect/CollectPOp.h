//
// Created by Yifei Yang on 10/13/22.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_COLLECT_COLLECTPOP_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_COLLECT_COLLECTPOP_H

#include <fpdb/executor/physical/PhysicalOp.h>
#include <fpdb/executor/message/CompleteMessage.h>
#include <fpdb/executor/message/TupleSetMessage.h>

using namespace fpdb::executor::message;
using namespace fpdb::tuple;

namespace fpdb::executor::physical::collect {

/**
 * Operator just forward tupleSets from multiple producers to a single consumer
 */
class CollectPOp : public PhysicalOp {

public:
  CollectPOp(const std::string &name,
             const std::vector<std::string> &projectColumnNames,
             int nodeId);
  CollectPOp() = default;
  CollectPOp(const CollectPOp&) = default;
  CollectPOp& operator=(const CollectPOp&) = default;

  void onReceive(const Envelope &msg) override;
  std::string getTypeString() const override;
  void clear() override;
  void produce(const std::shared_ptr<PhysicalOp> &op) override;
  void consume(const std::shared_ptr<PhysicalOp> &op) override;

  const std::vector<std::string> &getOrderedProducers() const;

private:
  void onStart();
  void onComplete(const CompleteMessage &);
  void onTupleSet(const TupleSetMessage &message);

  std::vector<std::string> orderedProducers_;   // maintain producers with incoming order

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, CollectPOp& op) {
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
                               f.field("orderedProducers", op.orderedProducers_));
  }
};

}


#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_COLLECT_COLLECTPOP_H
