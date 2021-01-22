//
// Created by Yifei Yang on 7/30/20.
//

#include "normal/cache/SegmentMetadata.h"

using namespace normal::cache;

SegmentMetadata::SegmentMetadata() :
  estimateSize_(0),
  size_(0),
  hitNum_(1),
  perSizeFreq_(0.0),
  value_(0.0),
  valid_(true) {}

SegmentMetadata::SegmentMetadata(size_t estimateSize, size_t size) :
  estimateSize_(estimateSize),
  size_(size),
  hitNum_(1),
  perSizeFreq_(0.0),
  value_(0.0),
  valid_(true) {}

SegmentMetadata::SegmentMetadata(const SegmentMetadata &m) :
  estimateSize_(m.estimateSize_),
  size_(m.size_),
  hitNum_(m.hitNum_),
  perSizeFreq_(m.perSizeFreq_),
  value_(m.value_),
  valid_(true) {}

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

void SegmentMetadata::incHitNum(size_t size) {
  hitNum_++;
  perSizeFreq_ = ((double) hitNum_) / ((double) size);
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

double SegmentMetadata::avgValue() const {
  return (hitNum_ == 0) ? 0.0 : value_ / ((double) hitNum_);
}

double SegmentMetadata::value2() const {
  double hitPara = 0.05;
  return hitPara * ((double) hitNum_) + value_;
}

void SegmentMetadata::addValue(double value) {
  value_ += value;
}

double SegmentMetadata::perSizeFreq() const {
  return perSizeFreq_;
}

bool SegmentMetadata::valid() const {
  return valid_;
}

void SegmentMetadata::invalidate() {
  valid_ = false;
}
