//
// Created by matt on 6/5/20.
//

#include "fpdb/tuple/TupleSetShowOptions.h"

using namespace fpdb::tuple;

TupleSetShowOptions::TupleSetShowOptions(TupleSetShowOrientation orientation, int maxNumRows) :
	orientation_(orientation),
	maxNumRows_(maxNumRows) {}

TupleSetShowOrientation TupleSetShowOptions::getOrientation() const {
  return orientation_;
}

int TupleSetShowOptions::getMaxNumRows() const {
  return maxNumRows_;
}
