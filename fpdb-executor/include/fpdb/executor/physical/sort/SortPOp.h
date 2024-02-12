//
// Created by Yifei Yang on 11/20/21.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_SORT_SORTPOP_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_SORT_SORTPOP_H

#include <fpdb/executor/physical/PhysicalOp.h>
#include <fpdb/executor/message/CompleteMessage.h>
#include <fpdb/executor/message/TupleSetMessage.h>
#include <fpdb/plan/prephysical/SortKey.h>
#include <fpdb/tuple/serialization/ArrowSerializer.h>
#include <arrow/compute/api.h>

using namespace fpdb::executor::message;
using namespace fpdb::plan::prephysical;
using namespace std;

namespace fpdb::executor::physical::sort {

class SortPOp : public PhysicalOp {

public:
  SortPOp(const string &name,
          const vector<string> &projectColumnNames,
          int nodeId,
          const vector<SortKey> &sortKeys);
  SortPOp() = default;
  SortPOp(const SortPOp&) = default;
  SortPOp& operator=(const SortPOp&) = default;

  void onReceive(const Envelope &msg) override;
  void clear() override;
  std::string getTypeString() const override;

private:
  void onStart();
  void onComplete(const CompleteMessage &);
  void onTupleSet(const TupleSetMessage &message);

  void makeArrowSortOptions();
  void buffer(const shared_ptr<TupleSet> &tupleSet);
  shared_ptr<TupleSet> sort();

  vector<SortKey> sortKeys_;
  std::optional<arrow::compute::SortOptions> arrowSortOptions_;
  std::optional<shared_ptr<TupleSet>> buffer_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, SortPOp& op) {
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
                               f.field("sortKeys", op.sortKeys_));
  }
};

}


#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_SORT_SORTPOP_H
