//
// Created by Yifei Yang on 4/20/22.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_GROUP_GROUPARROWKERNEL_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_GROUP_GROUPARROWKERNEL_H

#include <fpdb/executor/physical/group/GroupAbstractKernel.h>
#include <arrow/compute/exec/options.h>
#include <arrow/util/async_generator.h>

namespace fpdb::executor::physical::group {

struct GroupArrowExecPlanSuite {
  std::shared_ptr<arrow::compute::ExecContext> execContext_;
  std::shared_ptr<arrow::compute::ExecPlan> execPlan_;
  arrow::compute::ExecNode* dummyNode_;
  arrow::compute::ExecNode* aggregateNode_;
  arrow::compute::ExecNode* sinkNode_;
  std::shared_ptr<arrow::AsyncGenerator<arrow::util::optional<arrow::compute::ExecBatch>>> sinkGen_;
  int numInputBatches_;
};

class GroupArrowKernel: public GroupAbstractKernel{

public:
  GroupArrowKernel(const std::vector<std::string> &groupColumnNames,
                   const std::vector<std::shared_ptr<AggregateFunction>> &aggregateFunctions);
  GroupArrowKernel() = default;
  GroupArrowKernel(const GroupArrowKernel&) = default;
  GroupArrowKernel& operator=(const GroupArrowKernel&) = default;

  tl::expected<void, std::string> group(const std::shared_ptr<TupleSet> &tupleSet) override;
  tl::expected<std::shared_ptr<TupleSet>, std::string> finalise() override;
  void clear() override;

private:
  tl::expected<std::shared_ptr<TupleSet>, std::string> evaluateExpr(const std::shared_ptr<TupleSet> &tupleSet);
  tl::expected<void, std::string> doGroup(const std::shared_ptr<TupleSet> &tupleSet);
  tl::expected<std::shared_ptr<TupleSet>, std::string> finalizeAvg(const std::shared_ptr<TupleSet> &tupleSet);
  tl::expected<void, std::string> makeOutputSchema(const std::shared_ptr<arrow::Schema> &schema);
  tl::expected<void, std::string> makeArrowExecPlan(const std::shared_ptr<arrow::Schema> &schema);
  tl::expected<std::pair<arrow::FieldVector, arrow::ChunkedArrayVector>, std::string>
  getGroupColumns(const std::shared_ptr<TupleSet> &tupleSet);

  std::optional<std::shared_ptr<arrow::Schema>> outputSchema_;  // for Avg, outputSchema refers to the one before making
                                                                // division between sum and count column
  std::optional<arrow::compute::AggregateNodeOptions> aggregateNodeOptions_;
  std::optional<GroupArrowExecPlanSuite> arrowExecPlanSuite_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, GroupArrowKernel& kernel) {
    return f.object(kernel).fields(f.field("type", kernel.type_),
                                   f.field("groupColumnNames", kernel.groupColumnNames_),
                                   f.field("aggregateFunctions", kernel.aggregateFunctions_));
  }
};

}


#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_GROUP_GROUPARROWKERNEL_H
