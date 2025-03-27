//
// Created by Yifei Yang on 4/25/24.
//

#include <fpdb/executor/physical/transform/pred-trans/FilterSrc.h>
#include <algorithm>

namespace fpdb::executor::physical {

FilterSrc::FilterSrc(uint self): self_(self) {}

void FilterSrc::addLocal() {
  // local filter has no path
  filters_[self_].emplace(FilterPath{});
}

void FilterSrc::addTransferred(uint srcId, const FilterSrc& src, bool hasNaturalSelfFilter) {
  for (const auto &it: src.filters_) {
    uint origin = it.first;
    for (const auto &path: it.second) {
      FilterPath newPath = path;
      newPath.emplace_back(srcId);
      filters_[origin].emplace(newPath);
    }
  }
  // if the transfer itself is naturally a filter itself (i.e., neither pk-fk nor self-join)
  if (hasNaturalSelfFilter) {
    filters_[srcId].emplace(FilterPath{srcId});
  }
}

bool FilterSrc::contains(const FilterSrc& src, bool isSelfJoin, const fpdb::util::DisjointSet<uint> &unions) const {
  for (const auto &srcIt: src.filters_) {
    uint origin = srcIt.first;
    // for self-join, skip for local filter since it's same for both src and this
    if (isSelfJoin && origin == src.self_) {
      continue;
    }
    // find out all origins in this that are either have same id with src orign or the corresponding prePOp is identical
    std::vector<const FilterPathSet*> matchedThisPathSets;
    for (const auto &thisIt: filters_) {
      uint thisOrigin = thisIt.first;
      if (origin == thisOrigin || unions.checkUnion(origin, thisOrigin)) {
        matchedThisPathSets.emplace_back(&thisIt.second);
      }
    }
    // check if each src path is contained by some path in "matchedThisPathSets"
    for (const auto &srcPath: srcIt.second) {
      bool pathContained = false;
      for (const FilterPathSet* thisPathSet: matchedThisPathSets) {
        for (const auto &thisPath: *thisPathSet) {
          // check if "thisPath" is prefix of "srcPath"
          if (isPrefix(thisPath, srcPath, unions)) {
            pathContained = true;
            break;
          }
        }
        if (pathContained) {
          break;
        }
      }
      if (!pathContained) {
        return false;
      }
    }
  }
  return true;
}

bool FilterSrc::isPrefix(const FilterPath &thisPath, const FilterPath &srcPath,
                         const fpdb::util::DisjointSet<uint> &unions) const {
  // check if "thisPath" is prefix of "srcPath", in two ways
  if (thisPath.size() > srcPath.size()) {
    return false;
  }
  //  1) either ids are equal
  auto res = std::mismatch(thisPath.begin(), thisPath.end(), srcPath.begin());
  if (res.first == thisPath.end()) {
    return true;
  }
  //  2) or ids are different, but actually prePOps are identical
  for (uint i = 0; i < thisPath.size(); ++i) {
    if (!unions.checkUnion(thisPath[i], srcPath[i])) {
      return false;
    }
  }
  return true;
}

}
