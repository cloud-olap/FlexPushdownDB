//
// Created by matt on 1/8/20.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_HASHJOIN_HASHJOINBUILDKERNEL_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_HASHJOIN_HASHJOINBUILDKERNEL_H

#include <fpdb/tuple/TupleSet.h>
#include <fpdb/tuple/TupleSetIndex.h>
#include <string>
#include <memory>

using namespace fpdb::tuple;
using namespace std;

namespace fpdb::executor::physical::join {

/**
 * Kernel for creating the hash table on the build relation in a hash join
 */
class HashJoinBuildKernel {

public:
  explicit HashJoinBuildKernel(vector<string> columnNames);
  HashJoinBuildKernel() = default;
  HashJoinBuildKernel(const HashJoinBuildKernel&) = default;
  HashJoinBuildKernel& operator=(const HashJoinBuildKernel&) = default;
  static HashJoinBuildKernel make(const vector<string> &columnNames);

  tl::expected<void, string> put(const shared_ptr<TupleSet> &tupleSet);
  size_t size();
  void clear();
  std::optional<shared_ptr<TupleSetIndex>> getTupleSetIndex();

private:

  /**
   * The columns to hash on
   */
  vector<string> columnNames_;

  /**
   * The hashtable as an indexed tupleset
   */
  std::optional<shared_ptr<TupleSetIndex>> tupleSetIndex_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, HashJoinBuildKernel& kernel) {
    return f.apply(kernel.columnNames_);
  }
};

}

#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_HASHJOIN_HASHJOINBUILDKERNEL_H
