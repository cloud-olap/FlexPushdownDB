//
// Created by Yifei Yang on 2/20/24.
//

#ifndef FPDB_FPDB_TUPLE_INCLUDE_FPDB_TUPLE_UTIL_BLOOMFILTERMATHUTIL_H
#define FPDB_FPDB_TUPLE_INCLUDE_FPDB_TUPLE_UTIL_BLOOMFILTERMATHUTIL_H

namespace fpdb::tuple::util {

class BloomFilterMathUtil {

public:
  static int numBitsPerKey(int numHashFunc, double fpr);
  static int numBitsPerKeyDist(int numHashFunc, double fpr, int numNodes);
  static double fpr(int numHashFunc, int numBitsPerKey);
};

}

#endif //FPDB_FPDB_TUPLE_INCLUDE_FPDB_TUPLE_UTIL_BLOOMFILTERMATHUTIL_H
