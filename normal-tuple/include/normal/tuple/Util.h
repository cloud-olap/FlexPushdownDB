//
// Created by matt on 12/8/20.
//

#ifndef NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_UTIL_H
#define NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_UTIL_H

#include <vector>
#include <stdexcept>

namespace normal::tuple {

class Util {

public:

  /**
   * Given a start and finish number, will create pairs of numbers from start to finish (inclusive)
   * evenly split across the number of ranges given.
   *
   * E.g.
   * ranges(0,8,3) -> [[0,1,2][3,4,5][6,7,8]]
   * ranges(0,9,3) -> [[0,1,2,3][4,5,6,7][8,9]]
   * ranges(0,10,3) -> [[0,1,2,3][4,5,6,7][8,9,10]]
   *
   * @tparam T
   * @param start
   * @param finish
   * @param numRanges
   * @return
   */
  template<typename T>
  static std::vector<std::pair<T, T>> ranges(T start, T finish, T numRanges) {
	std::vector<std::pair<T, T>> result;

	T rangeSize = ((finish - start) / numRanges) + 1;

	for (int i = 0; i < numRanges; ++i) {
	  T rangeStart = rangeSize * i;
	  T rangeStop = std::min((rangeStart + rangeSize) - 1, finish);
	  result.push_back(std::pair{rangeStart, rangeStop});
	}

	return result;
  }
};

}

#endif //NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_UTIL_H
