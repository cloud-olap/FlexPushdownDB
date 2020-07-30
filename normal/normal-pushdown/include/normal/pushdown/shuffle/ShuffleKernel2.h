//
// Created by matt on 29/7/20.
//

#ifndef NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_SHUFFLE_SHUFFLEKERNEL2_H
#define NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_SHUFFLE_SHUFFLEKERNEL2_H

#include <vector>
#include <memory>
#include <string>

#include <tl/expected.hpp>

#include <normal/tuple/TupleSet2.h>

using namespace normal::tuple;

namespace normal::pushdown::shuffle {

/**
 * Shuffles a given tuple set into numSlots tuplesets based on the values in the column with the given name.
 */
class ShuffleKernel2 {

public:
  static tl::expected<std::vector<std::shared_ptr<TupleSet2>>, std::string>
  shuffle(const std::string &columnName, size_t numSlots, const TupleSet2 &tupleSet);

};

}

#endif //NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_SHUFFLE_SHUFFLEKERNEL2_H
