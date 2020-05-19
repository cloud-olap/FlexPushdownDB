//
// Created by matt on 19/5/20.
//

#include "normal/cache/SegmentKey.h"

using namespace normal::cache;

SegmentKey::SegmentKey(std::shared_ptr<Partition> Partition, SegmentRange Range)
	: partition_(std::move(Partition)), range_(Range) {}

std::shared_ptr<SegmentKey> SegmentKey::make(const std::shared_ptr<Partition> &Partition, SegmentRange Range) {
  return std::make_shared<SegmentKey>(Partition, Range);
}

const std::shared_ptr<Partition> &SegmentKey::getPartition() const {
  return partition_;
}

[[maybe_unused]] const SegmentRange &SegmentKey::getRange() const {
  return range_;
}

std::string SegmentKey::toString() {
  return fmt::format("({},{})", partition_->toString() , range_.toString());
}

bool SegmentKey::operator==(const SegmentKey &other) const {
  return this->partition_->equalTo(other.partition_) && this->range_ == other.range_;
}

bool SegmentKey::operator!=(const SegmentKey &other) const {
  return !(*this == other);
}

size_t SegmentKey::hash() {
  return partition_->hash() + range_.hash();
}
