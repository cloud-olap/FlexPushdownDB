//
// Created by matt on 28/4/20.
//

#ifndef NORMAL_NORMAL_SSB_INCLUDE_NORMAL_SSB_QUERIES_H
#define NORMAL_NORMAL_SSB_INCLUDE_NORMAL_SSB_QUERIES_H

#include <memory>

#include <normal/tuple/TupleSet.h>

/**
 * SSB query factories
 */
class Queries {

public:
  static std::string query01(short year, short discount, short quantity);
};

#endif //NORMAL_NORMAL_SSB_INCLUDE_NORMAL_SSB_QUERIES_H
