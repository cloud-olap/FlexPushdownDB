//
// Created by Yifei Yang on 4/21/23.
//

#include <fpdb/plan/prephysical/Util.h>
#include <fpdb/plan/prephysical/FilterableScanPrePOp.h>
#include <fpdb/plan/prephysical/GroupPrePOp.h>
#include <fpdb/plan/prephysical/ProjectPrePOp.h>
#include <fpdb/plan/prephysical/separable/SeparableSuperPrePOp.h>
#include <fpdb/util/Util.h>
#include <sstream>

namespace fpdb::plan::prephysical {

std::shared_ptr<FilterableScanPrePOp> Util::traceScanOriginWithNoJoinInPath(const std::shared_ptr<PrePhysicalOp> &op) {
  if (op->getType() == PrePOpType::FILTERABLE_SCAN || op->getType() == PrePOpType::SEPARABLE_SUPER) {
    return std::static_pointer_cast<FilterableScanPrePOp>(op);
  } else if (op->getType() == PrePOpType::HASH_JOIN || op->getType() == PrePOpType::NESTED_LOOP_JOIN) {
    return nullptr;
  } else {
    return traceScanOriginWithNoJoinInPath(op->getProducers()[0]);
  }
}
bool Util::hasLocalFilter(const std::shared_ptr<PrePhysicalOp> &op, const std::vector<std::string> &key) {
  switch (op->getType()) {
    case PrePOpType::FILTER:
    case PrePOpType::AGGREGATE:
    case PrePOpType::LIMIT_SORT: {
      return true;
    }
    case PrePOpType::FILTERABLE_SCAN: {
      return std::static_pointer_cast<FilterableScanPrePOp>(op)->getPredicate() != nullptr;
    }
    case PrePOpType::GROUP: {
      auto typedOp = std::static_pointer_cast<GroupPrePOp>(op);
      const auto &groupColumns = typedOp->getGroupColumnNames();
      std::unordered_set<std::string> groupColumnSet(groupColumns.begin(), groupColumns.end());
      std::unordered_set<std::string> keySet(key.begin(), key.end());
      if (fpdb::util::isSubSet(keySet, groupColumnSet)) {
        return hasLocalFilter(typedOp->getProducers()[0], key);
      } else {
        return true;
      }
    }
    case PrePOpType::PROJECT: {
      auto typedOp = std::static_pointer_cast<ProjectPrePOp>(op);
      // get a map for 'new name' <-> 'old name'
      std::unordered_map<std::string, std::string> newToOldName;
      for (const auto &rename: typedOp->getProjectColumnNamePairs()) {
        newToOldName[rename.second] = rename.first;
      }
      std::vector<std::string> newKey;
      for (const auto &col: key) {
        auto it = newToOldName.find(col);
        if (it != newToOldName.end()) {
          newKey.emplace_back(it->second);
        }
      }
      return hasLocalFilter(typedOp->getProducers()[0], newKey);
    }
    default: {
      return false;
    }
  }
}

std::vector<std::shared_ptr<PrePhysicalOp>> Util::findAllOfType(const std::shared_ptr<PrePhysicalOp> &op,
                                                                PrePOpType type) {
  std::vector<std::shared_ptr<PrePhysicalOp>> res;
  for (const auto &producer: op->getProducers()) {
    const auto &subRes = findAllOfType(producer, type);
    res.insert(res.end(), subRes.begin(), subRes.end());
  }
  if (op->getType() == type) {
    res.emplace_back(op);
  }
  return res;
}

std::string Util::getBaseTableDigest(const std::shared_ptr<PrePhysicalOp> &op) {
  std::vector<std::string> derives;
  std::string table;
  std::shared_ptr<PrePhysicalOp> currOp = op;
  while (true) {
    // unwrap if it's SeparableSuperPrePOp
    if (currOp->getType() == PrePOpType::SEPARABLE_SUPER) {
      currOp = std::static_pointer_cast<separable::SeparableSuperPrePOp>(currOp)->getRootOp();
    }
    // treat correspondingly
    if (currOp->getType() == PrePOpType::HASH_JOIN || currOp->getType() == PrePOpType::NESTED_LOOP_JOIN) {
      table = "<Join>";
      break;
    }
    if (currOp->getType() == PrePOpType::FILTERABLE_SCAN) {
      table = std::static_pointer_cast<FilterableScanPrePOp>(currOp)->getTable()->getName();
      break;
    }
    if (currOp->getType() == PrePOpType::FILTER) {
      derives.emplace_back("Filter");
    } else if (currOp->getType() == PrePOpType::LIMIT_SORT) {
      derives.emplace_back("LimitSort");
    } else if (currOp->getType() == PrePOpType::GROUP) {
      derives.emplace_back("Group");
    }
    // keep looking at its parent
    currOp = currOp->getProducers()[0];
  }
  std::stringstream ss;
  for (const auto &derive: derives) {
    ss << derive << "[";
  }
  ss << table;
  for (uint i = 0; i < derives.size(); ++i) {
    ss << "]";
  }
  return ss.str();
}

}
