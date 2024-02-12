//
// Created by Yifei Yang on 4/20/22.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_GROUP_GROUPABSTRACTKERNEL_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_GROUP_GROUPABSTRACTKERNEL_H

#include <fpdb/executor/physical/group/GroupKernelType.h>
#include <fpdb/executor/physical/aggregate/function/AggregateFunction.h>

using namespace fpdb::executor::physical::aggregate;

namespace fpdb::executor::physical::group {

class GroupAbstractKernel {

public:
  GroupAbstractKernel(GroupKernelType type,
                      const std::vector<std::string> &groupColumnNames,
                      const std::vector<std::shared_ptr<AggregateFunction>> &aggregateFunctions);
  GroupAbstractKernel() = default;
  GroupAbstractKernel(const GroupAbstractKernel&) = default;
  GroupAbstractKernel& operator=(const GroupAbstractKernel&) = default;
  virtual ~GroupAbstractKernel() = default;

  /**
   * Groups the input tuple set and computes intermediate aggregates
   *
   * @param tupleSet
   * @return
   */
  virtual tl::expected<void, std::string> group(const std::shared_ptr<TupleSet> &tupleSet) = 0;

  /**
   * Computes final aggregates and generates output tuple set
   *
   * @return
   */
  virtual tl::expected<std::shared_ptr<TupleSet>, std::string> finalise() = 0;

  /**
   * Clear internal state
   */
  virtual void clear() = 0;

  GroupKernelType getType() const;

  ::nlohmann::json toJson() const;
  static tl::expected<std::shared_ptr<GroupAbstractKernel>, std::string> fromJson(const nlohmann::json &jObj);

protected:
  GroupKernelType type_;
  std::vector<std::string> groupColumnNames_;
  std::vector<std::shared_ptr<AggregateFunction>> aggregateFunctions_;

};

}


#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_GROUP_GROUPABSTRACTKERNEL_H
