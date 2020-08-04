//
// Created by matt on 4/6/20.
//

#include "normal/cache/CachingPolicy.h"

using namespace normal::cache;

CachingPolicy::CachingPolicy(size_t maxSize) :
  maxSize_(maxSize), freeSize_(maxSize) {}
