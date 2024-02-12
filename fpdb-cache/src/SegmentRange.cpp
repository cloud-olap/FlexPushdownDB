//
// Created by matt on 19/5/20.
//

#include <fpdb/cache/SegmentRange.h>
#include <fmt/format.h>

using namespace fpdb::cache;

SegmentRange::SegmentRange(unsigned long StartOffset, unsigned long FinishOffset)
	: startOffset_(StartOffset), finishOffset_(FinishOffset) {}

SegmentRange SegmentRange::make(unsigned long StartOffset, unsigned long FinishOffset) {
  return SegmentRange(StartOffset, FinishOffset);
}

std::string SegmentRange::toString() {
  return fmt::format("({}:{})", startOffset_, finishOffset_);
}

bool SegmentRange::operator==(const SegmentRange &other) const {
  return this->startOffset_ == other.startOffset_ && this->finishOffset_ == other.finishOffset_;
}

size_t SegmentRange::hash() const {
  return  std::hash<unsigned long>()(startOffset_) + std::hash<unsigned long>()(finishOffset_);
}
