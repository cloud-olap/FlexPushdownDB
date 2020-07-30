//
// Created by matt on 29/7/20.
//

#ifndef NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_SHUFFLE_SHUFFLEKERNEL_H
#define NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_SHUFFLE_SHUFFLEKERNEL_H

#include <vector>
#include <memory>
#include <string>

#include <tl/expected.hpp>

#include <normal/tuple/TupleSet2.h>

using namespace normal::tuple;

namespace normal::pushdown::shuffle {

}
class ShuffleKernel {

public:
  static tl::expected<std::vector<std::shared_ptr<TupleSet2>>, std::string>
  shuffle(const std::string &columnName, size_t numPartitions, const std::shared_ptr<TupleSet2> &tupleSet);
};

#endif //NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_SHUFFLE_SHUFFLEKERNEL_H
