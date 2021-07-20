//
// Created by matt on 4/6/20.
//

#include "normal/cache/CachingPolicy.h"

#include <utility>

using namespace normal::cache;

CachingPolicy::CachingPolicy(size_t maxSize, std::shared_ptr<normal::plan::operator_::mode::Mode> mode) :
  mode_(std::move(mode)), maxSize_(maxSize), freeSize_(maxSize) {}

[[maybe_unused]] size_t CachingPolicy::getFreeSize() const {
  return freeSize_;
}
