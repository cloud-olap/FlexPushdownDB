//
// Created by matt on 1/8/20.
//

#ifndef NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_JOIN_HASHJOINBUILDKERNEL2_H
#define NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_JOIN_HASHJOINBUILDKERNEL2_H

#include <string>
#include <memory>

#include <normal/pushdown/join/HashTable.h>
#include "ArraySetIndex.h"

namespace normal::pushdown::join {

class HashJoinBuildKernel2 {

public:
  explicit HashJoinBuildKernel2(std::string columnName);

  static HashJoinBuildKernel2 make(const std::string &columnName);

  tl::expected<void,std::string> put(const std::shared_ptr<TupleSet2> &tupleSet);
  size_t size();
  void clear();

  std::shared_ptr<ArraySetIndex> getArraySetIndex();

private:

  /**
   * The column to hash on
   */
  std::string columnName_;

  /**
   * The hashtable
   */
  std::optional<std::shared_ptr<ArraySetIndex>> arraySetIndex_;

};

}

#endif //NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_JOIN_HASHJOINBUILDKERNEL2_H
