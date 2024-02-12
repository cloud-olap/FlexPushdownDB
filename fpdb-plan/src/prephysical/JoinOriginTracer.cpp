//
// Created by Yifei Yang on 4/14/23.
//

#include <fpdb/plan/prephysical/JoinOriginTracer.h>
#include <fpdb/plan/Globals.h>
#include <unordered_map>

namespace fpdb::plan::prephysical {

std::unordered_set<std::shared_ptr<JoinOrigin>, JoinOriginPtrHash, JoinOriginPtrPred>
JoinOriginTracer::trace(const std::shared_ptr<PrePhysicalPlan> &plan) {
  JoinOriginTracer tracer(plan);
  tracer.trace();
  return tracer.mergeSingleJoinOrigins();
}

JoinOriginTracer::JoinOriginTracer(const std::shared_ptr<PrePhysicalPlan> &plan):
  plan_(plan) {}

void JoinOriginTracer::trace() {
  std::vector<std::shared_ptr<ColumnOrigin>> emptyColumnOrigins{};
  traceDFS(plan_->getRootOp(), emptyColumnOrigins);
}

std::vector<std::shared_ptr<JoinOriginTracer::ColumnOrigin>>
JoinOriginTracer::traceDFS(const std::shared_ptr<PrePhysicalOp> &op,
                           const std::vector<std::shared_ptr<ColumnOrigin>> &columnOrigins) {
  switch (op->getType()) {
    case PrePOpType::AGGREGATE: {
      return traceAggregate(std::static_pointer_cast<AggregatePrePOp>(op), columnOrigins);
    }
    case PrePOpType::GROUP: {
      return traceGroup(std::static_pointer_cast<GroupPrePOp>(op), columnOrigins);
    }
    case PrePOpType::SORT: {
      return traceSort(std::static_pointer_cast<SortPrePOp>(op), columnOrigins);
    }
    case PrePOpType::LIMIT_SORT: {
      return traceLimitSort(std::static_pointer_cast<LimitSortPrePOp>(op), columnOrigins);
    }
    case PrePOpType::FILTERABLE_SCAN: {
      return traceFilterableScan(std::static_pointer_cast<FilterableScanPrePOp>(op), columnOrigins);
    }
    case PrePOpType::FILTER: {
      return traceFilter(std::static_pointer_cast<FilterPrePOp>(op), columnOrigins);
    }
    case PrePOpType::PROJECT: {
      return traceProject(std::static_pointer_cast<ProjectPrePOp>(op), columnOrigins);
    }
    case PrePOpType::HASH_JOIN: {
      return traceHashJoin(std::static_pointer_cast<HashJoinPrePOp>(op), columnOrigins);
    }
    case PrePOpType::NESTED_LOOP_JOIN: {
      return traceNestedLoopJoin(std::static_pointer_cast<NestedLoopJoinPrePOp>(op), columnOrigins);
    }
    default: {
      throw std::runtime_error(
              fmt::format("Unsupported operator type to trace join origin: '{}'", op->getTypeString()));
    }
  }
}

std::vector<std::shared_ptr<JoinOriginTracer::ColumnOrigin>>
JoinOriginTracer::traceFilterableScan(const std::shared_ptr<FilterableScanPrePOp> &op,
                                      const std::vector<std::shared_ptr<ColumnOrigin>> &columnOrigins) {
  std::vector<std::shared_ptr<ColumnOrigin>> localColumnOrigins;
  const auto &scanColumns = op->getProjectColumnNames();
  std::set<std::string> scanColumnSet(scanColumns.begin(), scanColumns.end());
  for (auto &columnOrigin: columnOrigins) {
    if (scanColumnSet.find(columnOrigin->currName_) != scanColumnSet.end()) {
      columnOrigin->originOp_ = op;
      localColumnOrigins.emplace_back(columnOrigin);
    }
  }
  return localColumnOrigins;
}

std::vector<std::shared_ptr<JoinOriginTracer::ColumnOrigin>>
JoinOriginTracer::traceFilter(const std::shared_ptr<FilterPrePOp> &op,
                              const std::vector<std::shared_ptr<ColumnOrigin>> &columnOrigins) {
  // all columns are valid for filter op
  auto localColumnOrigins = traceDFS(op->getProducers()[0], columnOrigins);

  // expand local filters before predicate transfer
  if (ENABLE_JOIN_ORIGIN_LOCAL_FILTER_EXPANSION) {
    for (const auto &columnOrigin: localColumnOrigins) {
      columnOrigin->originOp_ = op;
    }
  }

  // local column origins are unchanged
  return localColumnOrigins;
}

std::vector<std::shared_ptr<JoinOriginTracer::ColumnOrigin>>
JoinOriginTracer::traceSort(const std::shared_ptr<SortPrePOp> &op,
                            const std::vector<std::shared_ptr<ColumnOrigin>> &columnOrigins) {
  // all columns are valid for sort op
  auto localColumnOrigins = traceDFS(op->getProducers()[0], columnOrigins);

  // local column origins are unchanged
  // here we don't need to expand "local filter" since sort does not reduce cardinality
  return localColumnOrigins;
}

std::vector<std::shared_ptr<JoinOriginTracer::ColumnOrigin>>
JoinOriginTracer::traceLimitSort(const std::shared_ptr<LimitSortPrePOp> &op,
                                 const std::vector<std::shared_ptr<ColumnOrigin>> &columnOrigins) {
  // all columns are valid for limit sort op
  auto localColumnOrigins = traceDFS(op->getProducers()[0], columnOrigins);

  // expand local filters, since limit sort reduces cardinality
  if (ENABLE_JOIN_ORIGIN_LOCAL_FILTER_EXPANSION) {
    for (const auto &columnOrigin: localColumnOrigins) {
      if (columnOrigin->unappliedCurrName_.has_value()) {
        columnOrigin->currName_ = *columnOrigin->unappliedCurrName_;
        columnOrigin->unappliedCurrName_ = std::nullopt;
      }
      columnOrigin->originOp_ = op;
    }
  }

  // local column origins are unchanged
  return localColumnOrigins;
}

std::vector<std::shared_ptr<JoinOriginTracer::ColumnOrigin>>
JoinOriginTracer::traceProject(const std::shared_ptr<ProjectPrePOp> &op,
                               const std::vector<std::shared_ptr<ColumnOrigin>> &columnOrigins) {
  // get a map for 'new name' <-> 'old name'
  std::unordered_map<std::string, std::string> newToOldName, oldToNewName;
  for (const auto &rename: op->getProjectColumnNamePairs()) {
    newToOldName[rename.second] = rename.first;
    oldToNewName[rename.first] = rename.second;
  }

  // convert currName_ for input column origins
  for (const auto &columnOrigin: columnOrigins) {
    const auto &newToOldNameIt = newToOldName.find(columnOrigin->currName_);
    if (newToOldNameIt != newToOldName.end()) {
      columnOrigin->currName_ = newToOldNameIt->second;
    }
  }

  // trace the parent node
  auto localColumnOrigins = traceDFS(op->getProducers()[0], columnOrigins);

  // update local column origin based on renames
  // here we don't need to expand "local filter" since project does not reduce cardinality
  for (const auto &columnOrigin: localColumnOrigins) {
    auto oldToNewNameIt = oldToNewName.find(columnOrigin->currName_);
    if (oldToNewNameIt != oldToNewName.end()) {
      columnOrigin->unappliedCurrName_ = oldToNewNameIt->second;
    }
  }
  return localColumnOrigins;
}

std::vector<std::shared_ptr<JoinOriginTracer::ColumnOrigin>>
JoinOriginTracer::traceAggregate(const std::shared_ptr<AggregatePrePOp> &op,
                                 const std::vector<std::shared_ptr<ColumnOrigin>> &) {
  // aggregate op invalidates input column origins
  std::vector<std::shared_ptr<ColumnOrigin>> emptyColumnOrigins{};
  traceDFS(op->getProducers()[0], emptyColumnOrigins);

  // aggregate op invalidates local column origins
  return {};
}

std::vector<std::shared_ptr<JoinOriginTracer::ColumnOrigin>>
JoinOriginTracer::traceGroup(const std::shared_ptr<GroupPrePOp> &op,
                             const std::vector<std::shared_ptr<ColumnOrigin>> &columnOrigins) {
  // only columns in group keys are valid
  auto groupColumns = op->getGroupColumnNames();
  std::set<std::string> groupColumnSet(groupColumns.begin(), groupColumns.end());
  std::vector<std::shared_ptr<ColumnOrigin>> validColumnOrigins;
  for (const auto &columnOrigin: columnOrigins) {
    if (groupColumnSet.find(columnOrigin->currName_) != groupColumnSet.end()) {
      validColumnOrigins.emplace_back(columnOrigin);
    }
  }

  auto localColumnOrigins = traceDFS(op->getProducers()[0], validColumnOrigins);

  // expand local filters, since group reduces cardinality
  // local column origin are only group keys, but since the validation is already done above, we don't do again here
  for (const auto &columnOrigin: localColumnOrigins) {
    if (columnOrigin->unappliedCurrName_.has_value()) {
      columnOrigin->currName_ = *columnOrigin->unappliedCurrName_;
      columnOrigin->unappliedCurrName_ = std::nullopt;
    }
    columnOrigin->originOp_ = op;
  }
  return localColumnOrigins;
}

std::vector<std::shared_ptr<JoinOriginTracer::ColumnOrigin>>
JoinOriginTracer::traceHashJoin(const std::shared_ptr<HashJoinPrePOp> &op,
                                const std::vector<std::shared_ptr<ColumnOrigin>> &columnOrigins) {
  // construct left and right column origins
  std::vector<std::shared_ptr<ColumnOrigin>> leftColumnOrigins;
  std::vector<std::shared_ptr<ColumnOrigin>> rightColumnOrigins;
  for (const std::string &leftColumn: op->getLeftColumnNames()) {
    leftColumnOrigins.emplace_back(std::make_shared<ColumnOrigin>(leftColumn));
  }
  for (const std::string &rightColumn: op->getRightColumnNames()) {
    rightColumnOrigins.emplace_back(std::make_shared<ColumnOrigin>(rightColumn));
  }

  // we cannot distinguish which of input column origins are in left, which are in right, so need to add to both
  leftColumnOrigins.insert(leftColumnOrigins.end(), columnOrigins.begin(), columnOrigins.end());
  rightColumnOrigins.insert(rightColumnOrigins.end(), columnOrigins.begin(), columnOrigins.end());

  // trace parent nodes
  traceDFS(op->getProducers()[0], leftColumnOrigins);
  traceDFS(op->getProducers()[1], rightColumnOrigins);

  // construct join origins when trace is finished
  auto joinType = op->getJoinType();
  for (int i = 0; i < op->getNumJoinColumnPairs(); ++i) {
    const auto &leftColumnOriginOp = leftColumnOrigins[i]->originOp_;
    const auto &rightColumnOriginOp = rightColumnOrigins[i]->originOp_;
    const auto &leftColumn = leftColumnOrigins[i]->currName_;
    const auto &rightColumn = rightColumnOrigins[i]->currName_;
    if (leftColumnOriginOp != nullptr && rightColumnOriginOp != nullptr) {
      if (leftColumnOriginOp->getRowCount() <= rightColumnOriginOp->getRowCount()) {
        singleJoinOrigins_.emplace_back(std::make_shared<SingleJoinOrigin>(
                leftColumnOriginOp, rightColumnOriginOp, leftColumn, rightColumn, joinType));
      } else {
        auto expReversedJoinType = reverseJoinType(joinType);
        if (!expReversedJoinType.has_value()) {
          throw std::runtime_error(expReversedJoinType.error());
        }
        singleJoinOrigins_.emplace_back(std::make_shared<SingleJoinOrigin>(
                rightColumnOriginOp, leftColumnOriginOp, rightColumn, leftColumn, *expReversedJoinType));
      }
    }
  }

  // join blocks "local filters" expansion
  return {};
}

std::vector<std::shared_ptr<JoinOriginTracer::ColumnOrigin>>
JoinOriginTracer::traceNestedLoopJoin(const std::shared_ptr<NestedLoopJoinPrePOp> &op,
                                      const std::vector<std::shared_ptr<ColumnOrigin>> &columnOrigins) {
  // we cannot distinguish which of input column origins are in left, which are in right, so need to add to both
  traceDFS(op->getProducers()[0], columnOrigins);
  traceDFS(op->getProducers()[1], columnOrigins);

  // join blocks "local filters" expansion
  return {};
}

std::unordered_set<std::shared_ptr<JoinOrigin>, JoinOriginPtrHash, JoinOriginPtrPred>
JoinOriginTracer::mergeSingleJoinOrigins() {
  std::unordered_set<std::shared_ptr<JoinOrigin>, JoinOriginPtrHash, JoinOriginPtrPred> joinOrigins;
  for (const auto &singleJoinOrigin: singleJoinOrigins_) {
    auto newJoinOrigin = std::make_shared<JoinOrigin>(
            singleJoinOrigin->left_, singleJoinOrigin->right_, singleJoinOrigin->joinType_);
    auto joinOriginIt = joinOrigins.find(newJoinOrigin);
    if (joinOriginIt != joinOrigins.end()) {
      (*joinOriginIt)->addJoinColumnPair(singleJoinOrigin->leftColumn_, singleJoinOrigin->rightColumn_);
    } else {
      newJoinOrigin->addJoinColumnPair(singleJoinOrigin->leftColumn_, singleJoinOrigin->rightColumn_);
      joinOrigins.emplace(newJoinOrigin);
    }
  }
  return joinOrigins;
}

}
