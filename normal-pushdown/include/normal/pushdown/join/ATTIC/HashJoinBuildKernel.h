//
// Created by matt on 31/7/20.
//

#ifndef NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_JOIN_HASHJOINBUILDKERNEL_H
#define NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_JOIN_HASHJOINBUILDKERNEL_H

#include <string>
#include <memory>

#include <normal/pushdown/join/ATTIC/HashTable.h>

namespace normal::pushdown::join {

class HashJoinBuildKernel {

public:
  explicit HashJoinBuildKernel(std::string columnName);

  static HashJoinBuildKernel make(const std::string &columnName);

  tl::expected<void,std::string> put(const std::shared_ptr<TupleSet2> &tupleSet);
  size_t size();
  void clear();

  std::shared_ptr<HashTable> getHashTable();

private:

  /**
   * The column to hash on
   */
  std::string columnName_;

  /**
   * The hashtable
   */
  std::shared_ptr<HashTable> hashtable_;

};

}

#endif //NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_JOIN_HASHJOINBUILDKERNEL_H
