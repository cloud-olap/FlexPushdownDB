//
// Created by Yifei Yang on 4/25/24.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_TRANSFORM_PRED_TRANS_FILTERSRC_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_TRANSFORM_PRED_TRANS_FILTERSRC_H

#include <fpdb/util/DisjointSet.h>
#include <vector>
#include <set>
#include <unordered_map>
#include <sys/types.h>

namespace fpdb::executor::physical {

/**
 * Record so-far received filters (both local and transformed) during pred-trans, using prepOpId (self denotes local),
 * used for pruning unuseful pred-trans steps.
 */
class FilterSrc {
public:
  FilterSrc(uint self);

  void addLocal();
  void addTransferred(uint srcId, const FilterSrc& src, bool hasNaturalSelfFilter);
  bool contains(const FilterSrc& src, bool isSelfJoin, const fpdb::util::DisjointSet<uint> &unions) const;

private:
  using FilterPath = std::vector<uint>;
  using FilterPathSet = std::set<FilterPath>;

  bool isPrefix(const FilterPath &thisPath, const FilterPath &srcPath,
                const fpdb::util::DisjointSet<uint> &unions) const;

  uint self_;
  std::unordered_map<uint, FilterPathSet> filters_;   // represent for each filter src, all the paths to the current dst
};

}

#endif // FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_TRANSFORM_PRED_TRANS_FILTERSRC_H
