//
// Created by matt on 19/5/20.
//

#include "normal/cache/SegmentData.h"

using namespace normal::cache;

SegmentData::SegmentData(std::shared_ptr<Column> column) : column_(std::move(column)) {}

std::shared_ptr <SegmentData> SegmentData::make(const std::shared_ptr <Column> &column) {
  return std::make_shared<SegmentData>(column);
}

const std::shared_ptr<Column> &SegmentData::getColumn() const {
  return column_;
}
