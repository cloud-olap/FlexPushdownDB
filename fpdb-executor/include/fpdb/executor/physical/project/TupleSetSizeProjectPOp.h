//
// Created by Yifei Yang on 4/17/24.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_PROJECT_TUPLESETSIZEPROJECTPOP_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_PROJECT_TUPLESETSIZEPROJECTPOP_H

#include <fpdb/executor/physical/PhysicalOp.h>

namespace fpdb::executor::physical::project {

/**
 * Collect input size which is then used by downstream ops
 */
class TupleSetSizeProjectPOp: public PhysicalOp {
public:
  TupleSetSizeProjectPOp(const std::string &name,
                         const std::vector<std::string> &projectColumnNames,
                         int nodeId);
  TupleSetSizeProjectPOp() = default;
  TupleSetSizeProjectPOp(const TupleSetSizeProjectPOp&) = default;
  TupleSetSizeProjectPOp& operator=(const TupleSetSizeProjectPOp&) = default;
  ~TupleSetSizeProjectPOp() override = default;

  void onReceive(const Envelope &msg) override;
  std::string getTypeString() const override;
  void clear() override;

private:
  void onStart();
  void onTupleSet(const TupleSetMessage &msg);
  void onComplete(const CompleteMessage &);

  int64_t numRows_ = 0;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, TupleSetSizeProjectPOp& op) {
    return inspect_base(f, op);
  }
};

}

#endif // FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_PROJECT_TUPLESETSIZEPROJECTPOP_H
