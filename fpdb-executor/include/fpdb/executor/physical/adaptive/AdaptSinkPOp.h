//
// Created by Yifei Yang on 4/5/24.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_ADAPTIVE_ADAPTSINKPOP_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_ADAPTIVE_ADAPTSINKPOP_H

#include <fpdb/executor/physical/PhysicalOp.h>

namespace fpdb::executor::physical::adaptive {

/**
 * represent the end of a stage during adaptive query execution, store the results of a stage
 */
class AdaptSinkPOp: public PhysicalOp {

public:
  AdaptSinkPOp(const std::string &name,
               const std::vector<std::string> &projectColumnNames,
               int nodeId);
  AdaptSinkPOp() = default;
  AdaptSinkPOp(const AdaptSinkPOp&) = default;
  AdaptSinkPOp& operator=(const AdaptSinkPOp&) = default;
  ~AdaptSinkPOp() override = default;

  void onReceive(const Envelope &msg) override;
  std::string getTypeString() const override;
  void clear() override;

private:
  void onStart();
  void onTupleSet(const TupleSetMessage &msg);
  void onAdaptResume(const AdaptResumeMessage &msg);
  void onComplete(const CompleteMessage &);

  std::shared_ptr<TupleSet> result_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, AdaptSinkPOp& op) {
    return inspect_base(f, op);
  }
};

}

#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_ADAPTIVE_ADAPTSINKPOP_H
