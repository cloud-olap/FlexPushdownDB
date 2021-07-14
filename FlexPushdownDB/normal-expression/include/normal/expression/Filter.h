//
// Created by matt on 6/5/20.
//

#ifndef NORMAL_NORMAL_EXPRESSION_INCLUDE_NORMAL_EXPRESSION_FILTER_H
#define NORMAL_NORMAL_EXPRESSION_INCLUDE_NORMAL_EXPRESSION_FILTER_H

#include <memory>

#include <normal/tuple/Schema.h>
#include <normal/tuple/TupleSet2.h>

namespace normal::expression {

class Filter {

public:
  virtual ~Filter() = default;

  virtual std::shared_ptr<normal::tuple::TupleSet2> evaluate(const normal::tuple::TupleSet2 &TupleSet) = 0;

  virtual void compile(const std::shared_ptr<normal::tuple::Schema> &Schema) = 0;

};

}

#endif //NORMAL_NORMAL_EXPRESSION_INCLUDE_NORMAL_EXPRESSION_FILTER_H
