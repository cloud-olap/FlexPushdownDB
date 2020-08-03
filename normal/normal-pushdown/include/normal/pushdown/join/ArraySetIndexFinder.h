//
// Created by matt on 3/8/20.
//

#ifndef NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_JOIN_ARRAYSETINDEXFINDER_H
#define NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_JOIN_ARRAYSETINDEXFINDER_H

#include <vector>
#include <cstdint>

class ArraySetIndexFinder {
public:

  ArraySetIndexFinder() = default;
  virtual ~ArraySetIndexFinder() = default;

  virtual std::vector<int64_t> find(int64_t i) = 0;
};

#endif //NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_JOIN_ARRAYSETINDEXFINDER_H
