//
// Created by matt on 6/5/20.
//

#include "TupleSetShowOptions.h"

using namespace normal::tuple;

TupleSetShowOptions::TupleSetShowOptions(TupleSetShowOrientation orientation) : orientation_(orientation) {}

TupleSetShowOrientation TupleSetShowOptions::getOrientation() const {
  return orientation_;
}