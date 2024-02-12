//
// Created by Yifei Yang on 12/6/21.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_SORT_LIMITSORTPOP_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_SORT_LIMITSORTPOP_H

#include <fpdb/executor/physical/PhysicalOp.h>
#include <fpdb/executor/message/CompleteMessage.h>
#include <fpdb/executor/message/TupleSetMessage.h>
#include <fpdb/plan/prephysical/SortKey.h>
#include <fpdb/tuple/serialization/ArrowSerializer.h>
#include <arrow/compute/api.h>
#include <memory>

using namespace fpdb::executor::message;
using namespace fpdb::plan::prephysical;
using namespace std;

namespace fpdb::executor::physical::limitsort {

class LimitSortPOp : public PhysicalOp {

public:
  LimitSortPOp(const string &name,
               const vector<string> &projectColumnNames,
               int nodeId,
               int64_t k,
               const vector<SortKey> &sortKeys);
  LimitSortPOp() = default;
  LimitSortPOp(const LimitSortPOp&) = default;
  LimitSortPOp& operator=(const LimitSortPOp&) = default;

  void onReceive(const Envelope &msg) override;
  void clear() override;
  std::string getTypeString() const override;

private:
  void onStart();
  void onComplete(const CompleteMessage &);
  void onTupleSet(const TupleSetMessage &message);

  void makeArrowSelectKOptions();
  shared_ptr<TupleSet> makeInput(const shared_ptr<TupleSet> &tupleSet);
  shared_ptr<TupleSet> selectK(const shared_ptr<TupleSet> &tupleSet);

  int64_t k_;
  vector<SortKey> sortKeys_;
  std::optional<arrow::compute::SelectKOptions> arrowSelectKOptions_;
  std::optional<shared_ptr<TupleSet>> result_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, LimitSortPOp& op) {
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
                               f.field("k", op.k_),
                               f.field("sortKeys", op.sortKeys_));
  }
};

}


#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_SORT_LIMITSORTPOP_H
