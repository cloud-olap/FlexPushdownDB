//
// Created by Yifei Yang on 7/30/20.
//

#include <fpdb/cache/SegmentMetadata.h>

using namespace fpdb::cache;

SegmentMetadata::SegmentMetadata(size_t size) :
  size_(size),
  hitNum_(1),
  perSizeFreq_(0.0),
  value_(0.0),
  valid_(true) {}

SegmentMetadata::SegmentMetadata(const SegmentMetadata &m) :
  size_(m.size_),
  hitNum_(m.hitNum_),
  perSizeFreq_(m.perSizeFreq_),
  value_(m.value_),
  valid_(true) {}

std::shared_ptr<SegmentMetadata> fpdb::cache::SegmentMetadata::make(size_t size) {
  return std::make_shared<SegmentMetadata>(size);
}

size_t SegmentMetadata::size() const {
  return size_;
}

int SegmentMetadata::hitNum() const {
  return hitNum_;
}

double SegmentMetadata::perSizeFreq() const {
  return perSizeFreq_;
}

double SegmentMetadata::value() const {
  return value_;
}

bool SegmentMetadata::valid() const {
  return valid_;
}

void SegmentMetadata::setSize(size_t size) {
  size_ = size;
}

void SegmentMetadata::incHitNum() {
  hitNum_++;
}

void SegmentMetadata::incHitNum(size_t size) {
  hitNum_++;
  perSizeFreq_ = ((double) hitNum_) / ((double) size);
}

void SegmentMetadata::addValue(double value) {
  value_ += value;
}

void SegmentMetadata::invalidate() {
  valid_ = false;
}
