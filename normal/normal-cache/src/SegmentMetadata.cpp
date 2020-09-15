//
// Created by Yifei Yang on 7/30/20.
//

#include "normal/cache/SegmentMetadata.h"

using namespace normal::cache;

SegmentMetadata::SegmentMetadata() :
  estimateSize_(0),
  size_(0),
  hitNum_(1),
  value_(0.0) {}

SegmentMetadata::SegmentMetadata(size_t estimateSize, size_t size) :
  estimateSize_(estimateSize),
  size_(size),
  hitNum_(1),
  value_(0.0) {}

std::shared_ptr<SegmentMetadata> SegmentMetadata::make() {
  return std::make_shared<SegmentMetadata>();
}

std::shared_ptr<SegmentMetadata> normal::cache::SegmentMetadata::make(size_t estimateSize, size_t size) {
  return std::make_shared<SegmentMetadata>(estimateSize, size);
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

size_t SegmentMetadata::estimateSize() const {
  return estimateSize_;
}

double SegmentMetadata::value() const {
  return value_;
}

void SegmentMetadata::addValue(double value) {
  value_ += value;
}
