//
// Created by matt on 17/6/20.
//

#ifndef NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_SHUFFLE_SHUFFLER_H
#define NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_SHUFFLE_SHUFFLER_H

#include <vector>
#include <memory>
#include <string>

#include <normal/tuple/TupleSet2.h>

using namespace normal::tuple;

namespace normal::pushdown::shuffle {

class Shuffler {

public:

  /**
   * Shuffles the given tuple set into numPartitions tuple sets by computing the modulus of the hash of the
   * value in the column specified by column name and numPartitions.
   *
   * The returned tuple sets may be empty.
   *
   * @param columnName
   * @param numPartitions
   * @param tupleSet
   * @return
   */
  static tl::expected<std::vector<std::shared_ptr<TupleSet2>>, std::string>
  shuffle(const std::string &columnName, size_t numPartitions, const std::shared_ptr<TupleSet2> &tupleSet);

};

}

#endif //NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_SHUFFLE_SHUFFLER_H
