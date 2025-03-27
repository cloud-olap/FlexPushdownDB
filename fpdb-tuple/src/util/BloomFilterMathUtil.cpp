//
// Created by Yifei Yang on 2/20/24.
//

#include <fpdb/tuple/util/BloomFilterMathUtil.h>
#include <cmath>

namespace fpdb::tuple::util {

int BloomFilterMathUtil::numBitsPerKey(int numHashFunc, double fpr) {
  double numBitsPerKey = ((double) -numHashFunc) / log(1 - pow(fpr, 1 / (double) numHashFunc));
  return ceil(numBitsPerKey);
}

int BloomFilterMathUtil::numBitsPerKeyDist(int numHashFunc, double fpr, int numNodes) {
  double numBitsPerKey = ((double) -numHashFunc) /
          log(1 - pow(1 - pow(1 - fpr, 1 / (double) numNodes), 1 / (double) numHashFunc));
  return ceil(numBitsPerKey);
}

double BloomFilterMathUtil::fpr(int numHashFunc, int numBitsPerKey) {
  return pow(1 - exp(-(double) numHashFunc / numBitsPerKey), (double) numHashFunc);
}

}
