//
// Created by matt on 29/7/20.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_SHUFFLE_SHUFFLEKERNEL_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_SHUFFLE_SHUFFLEKERNEL_H

#include <fpdb/tuple/TupleSet.h>
#include <tl/expected.hpp>
#include <vector>
#include <memory>
#include <string>

using namespace fpdb::tuple;
using namespace std;

namespace fpdb::executor::physical::shuffle {

/**
 * Shuffles a given tuple set into numSlots tuplesets based on the values in the column with the given name.
 */
class ShuffleKernel {

public:
  static tl::expected<vector<shared_ptr<TupleSet>>, string>
  shuffle(const vector<string> &columnNames, size_t numSlots, const TupleSet &tupleSet);

};

}

#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_SHUFFLE_SHUFFLEKERNEL_H
