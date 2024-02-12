//
// Created by Yifei Yang on 4/14/23.
//

#ifndef FPDB_FPDB_PLAN_INCLUDE_FPDB_PLAN_PREPHYSICAL_JOINORIGINTRACER_H
#define FPDB_FPDB_PLAN_INCLUDE_FPDB_PLAN_PREPHYSICAL_JOINORIGINTRACER_H

#include <fpdb/plan/prephysical/PrePhysicalPlan.h>
#include <fpdb/plan/prephysical/FilterableScanPrePOp.h>
#include <fpdb/plan/prephysical/AggregatePrePOp.h>
#include <fpdb/plan/prephysical/GroupPrePOp.h>
#include <fpdb/plan/prephysical/SortPrePOp.h>
#include <fpdb/plan/prephysical/LimitSortPrePOp.h>
#include <fpdb/plan/prephysical/FilterableScanPrePOp.h>
#include <fpdb/plan/prephysical/FilterPrePOp.h>
#include <fpdb/plan/prephysical/ProjectPrePOp.h>
#include <fpdb/plan/prephysical/HashJoinPrePOp.h>
#include <fpdb/plan/prephysical/NestedLoopJoinPrePOp.h>
#include <fpdb/plan/prephysical/separable/SeparableSuperPrePOp.h>
#include <fpdb/util/Util.h>
#include <unordered_set>

namespace fpdb::plan::prephysical {
  
struct JoinOrigin {
  JoinOrigin(const std::shared_ptr<PrePhysicalOp> &left,
             const std::shared_ptr<PrePhysicalOp> &right,
             JoinType joinType):
    left_(left), right_(right),
    joinType_(joinType) {}
    
  void addJoinColumnPair(const std::string &leftColumn, const std::string &rightColumn) {
    leftColumns_.emplace_back(leftColumn);
    rightColumns_.emplace_back(rightColumn);
  }
  
  size_t hash() const {
    return fpdb::util::hashCombine({left_->getId(), right_->getId(), joinType_});
  }

  bool equalTo(const std::shared_ptr<JoinOrigin> &other) const {
    return left_->getId() == other->left_->getId() && right_->getId() == other->right_->getId()
        && joinType_ == other->joinType_;
  }
  
  std::shared_ptr<PrePhysicalOp> left_, right_;
  std::vector<std::string> leftColumns_, rightColumns_;
  JoinType joinType_;
};

struct JoinOriginPtrHash {
  inline size_t operator()(const std::shared_ptr<JoinOrigin> &joinOrigin) const {
    return joinOrigin->hash();
  }
};

struct JoinOriginPtrPred {
  inline bool operator()(const std::shared_ptr<JoinOrigin> &lhs, const std::shared_ptr<JoinOrigin> &rhs) const {
    return lhs->equalTo(rhs);
  }
};

class JoinOriginTracer {

public:
  static std::unordered_set<std::shared_ptr<JoinOrigin>, JoinOriginPtrHash, JoinOriginPtrPred>
  trace(const std::shared_ptr<PrePhysicalPlan> &plan);

private:
  struct SingleJoinOrigin {
    SingleJoinOrigin(const std::shared_ptr<PrePhysicalOp> &left,
                     const std::shared_ptr<PrePhysicalOp> &right,
                     const std::string &leftColumn,
                     const std::string &rightColumn,
                     JoinType joinType):
      left_(left), right_(right),
      leftColumn_(leftColumn), rightColumn_(rightColumn),
      joinType_(joinType) {}

    std::shared_ptr<PrePhysicalOp> left_, right_;
    std::string leftColumn_, rightColumn_;
    JoinType joinType_;
  };
  
  struct ColumnOrigin {
    ColumnOrigin(const std::string &name):
      name_(name), currName_(name) {}

    std::string name_;      // original name from the join op
    std::string currName_;  // there may be project ops that rename columns, so need to keep track of the current name
    std::optional<std::string> unappliedCurrName_ = std::nullopt;   // latest currName_, but has not been applied with the change of originOp_
                                                                    // e.g. in ProjectPrePOp, this is not used until "local filter" is further expanded to "group/limit sort"
    std::shared_ptr<PrePhysicalOp> originOp_ = nullptr;
  };

  JoinOriginTracer(const std::shared_ptr<PrePhysicalPlan> &plan);
  
  void trace();
  // return column origins that can be further processed by "local filters" (filter/project/group...) before joins
  std::vector<std::shared_ptr<ColumnOrigin>> traceDFS(const std::shared_ptr<PrePhysicalOp> &op,
                                                      const std::vector<std::shared_ptr<ColumnOrigin>> &columnOrigins);
  std::vector<std::shared_ptr<ColumnOrigin>> traceFilterableScan(const std::shared_ptr<FilterableScanPrePOp> &op,
                                                                 const std::vector<std::shared_ptr<ColumnOrigin>> &columnOrigins);
  std::vector<std::shared_ptr<ColumnOrigin>> traceFilter(const std::shared_ptr<FilterPrePOp> &op,
                                                         const std::vector<std::shared_ptr<ColumnOrigin>> &columnOrigins);
  std::vector<std::shared_ptr<ColumnOrigin>> traceSort(const std::shared_ptr<SortPrePOp> &op,
                                                       const std::vector<std::shared_ptr<ColumnOrigin>> &columnOrigins);
  std::vector<std::shared_ptr<ColumnOrigin>> traceLimitSort(const std::shared_ptr<LimitSortPrePOp> &op,
                                                            const std::vector<std::shared_ptr<ColumnOrigin>> &columnOrigins);
  std::vector<std::shared_ptr<ColumnOrigin>> traceProject(const std::shared_ptr<ProjectPrePOp> &op,
                                                          const std::vector<std::shared_ptr<ColumnOrigin>> &columnOrigins);
  std::vector<std::shared_ptr<ColumnOrigin>> traceAggregate(const std::shared_ptr<AggregatePrePOp> &op,
                                                            const std::vector<std::shared_ptr<ColumnOrigin>> &columnOrigins);
  std::vector<std::shared_ptr<ColumnOrigin>> traceGroup(const std::shared_ptr<GroupPrePOp> &op,
                                                        const std::vector<std::shared_ptr<ColumnOrigin>> &columnOrigins);
  std::vector<std::shared_ptr<ColumnOrigin>> traceHashJoin(const std::shared_ptr<HashJoinPrePOp> &op,
                                                           const std::vector<std::shared_ptr<ColumnOrigin>> &columnOrigins);
  std::vector<std::shared_ptr<ColumnOrigin>> traceNestedLoopJoin(const std::shared_ptr<NestedLoopJoinPrePOp> &op,
                                                                 const std::vector<std::shared_ptr<ColumnOrigin>> &columnOrigins);

  std::unordered_set<std::shared_ptr<JoinOrigin>, JoinOriginPtrHash, JoinOriginPtrPred> mergeSingleJoinOrigins();

  std::shared_ptr<PrePhysicalPlan> plan_;
  std::vector<std::shared_ptr<SingleJoinOrigin>> singleJoinOrigins_;
};

}


#endif //FPDB_FPDB_PLAN_INCLUDE_FPDB_PLAN_PREPHYSICAL_JOINORIGINTRACER_H
