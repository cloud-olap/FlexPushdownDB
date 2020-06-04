//
// Created by matt on 4/6/20.
//

#ifndef NORMAL_NORMAL_CACHE_INCLUDE_NORMAL_CACHE_CACHINGPOLICY_H
#define NORMAL_NORMAL_CACHE_INCLUDE_NORMAL_CACHE_CACHINGPOLICY_H

#include <memory>

#include "SegmentKey.h"

namespace normal::cache {

class CachingPolicy {

public:

  virtual ~CachingPolicy() = default;

  /**
   * Fired on load of an entry
   *
   * @param key
   */
  virtual void onLoad(const std::shared_ptr<SegmentKey> &key) = 0;

   /**
    * Fired on storage of an entry
    *
    * @param key
    * @return An optional key to remove from the cache
    */
  virtual std::optional<std::shared_ptr<SegmentKey>> onStore(const std::shared_ptr<SegmentKey> &key) = 0;

  /**
   * Fired on removal of on entry
   *
   * @param key
   */
  virtual void onRemove(const std::shared_ptr<SegmentKey> &key) = 0;

};

}

#endif //NORMAL_NORMAL_CACHE_INCLUDE_NORMAL_CACHE_CACHINGPOLICY_H
