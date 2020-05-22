//
// Created by matt on 22/5/20.
//

#ifndef NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_SAMPLE_H
#define NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_SAMPLE_H

#include <memory>

#include "TupleSet2.h"

namespace normal::tuple {

/**
 * Pre built sample tuple sets, useful for testing
 */
class Sample {

public:

  /**
   * 3 x 3 tuple set of strings
   *
   * @return
   */
  static std::shared_ptr<normal::tuple::TupleSet2> sample3x3String();
};

}

#endif //NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_SAMPLE_H
