//
// Created by Yifei Yang on 7/30/20.
//

#include "normal/cache/SegmentMetadata.h"

using namespace normal::cache;

SegmentMetadata::SegmentMetadata() :
  size_(0),
  hitNum_(1) {}

SegmentMetadata::SegmentMetadata(size_t size) :
  size_(size),
  hitNum_(1) {}

std::shared_ptr<SegmentMetadata> SegmentMetadata::make() {
  return std::make_shared<SegmentMetadata>();
}

std::shared_ptr<SegmentMetadata> normal::cache::SegmentMetadata::make(size_t size) {
  return std::make_shared<SegmentMetadata>(size);
}

size_t SegmentMetadata::size() const {
  return size_;
}

int SegmentMetadata::hitNum() const {
  return hitNum_;
}

void SegmentMetadata::incHitNum() {
  hitNum_++;
}

void SegmentMetadata::setSize(size_t size) {
  size_ = size;
}
