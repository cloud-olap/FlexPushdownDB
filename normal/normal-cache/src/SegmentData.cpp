//
// Created by matt on 19/5/20.
//

#include "normal/cache/SegmentData.h"

using namespace normal::cache;

SegmentData::SegmentData(std::shared_ptr<normal::tuple::TupleSet2> TupleSet) : tupleSet_(std::move(TupleSet)) {}

std::shared_ptr <SegmentData> SegmentData::make(const std::shared_ptr <normal::tuple::TupleSet2> &TupleSet) {
  return std::make_shared<SegmentData>(TupleSet);
}

const std::shared_ptr<normal::tuple::TupleSet2> &SegmentData::getTupleSet() const {
  return tupleSet_;
}
