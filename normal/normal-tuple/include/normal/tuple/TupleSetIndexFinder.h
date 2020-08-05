//
// Created by matt on 3/8/20.
//

#ifndef NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_TUPLESETINDEXFINDER_H
#define NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_TUPLESETINDEXFINDER_H

#include <vector>
#include <cstdint>

/**
 * A (type-erased) class holding an array and a tuple set index. Provides a find method allowing the caller
 * to look up a value in the array by row number and use that to look up the value in the index without the caller
 * needing to know the types involved.
 */
namespace normal::tuple {

class TupleSetIndexFinder {
public:

  TupleSetIndexFinder() = default;
  virtual ~TupleSetIndexFinder() = default;

  virtual std::vector<int64_t> find(int64_t rowIndex) = 0;

};

}

#endif //NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_TUPLESETINDEXFINDER_H
