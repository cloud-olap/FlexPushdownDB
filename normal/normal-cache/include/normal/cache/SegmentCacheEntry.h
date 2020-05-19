//
// Created by matt on 19/5/20.
//

#ifndef NORMAL_NORMAL_CACHE_INCLUDE_NORMAL_CACHE_SEGMENTCACHEENTRY_H
#define NORMAL_NORMAL_CACHE_INCLUDE_NORMAL_CACHE_SEGMENTCACHEENTRY_H

#include <memory>

#include "SegmentKey.h"
#include "SegmentData.h"

namespace normal::cache {

class SegmentCacheEntry {

public:

  SegmentCacheEntry(std::shared_ptr<SegmentKey> Key,
					std::shared_ptr<SegmentData> Data,
					const std::chrono::system_clock::time_point &LastUsedTimeStamp,
					long UsedCount);

  static std::shared_ptr<SegmentCacheEntry> make(const std::shared_ptr<SegmentKey> &Key,
												 const std::shared_ptr<SegmentData> &Data);

  [[nodiscard]] const std::shared_ptr<SegmentKey> &getKey() const;
  [[nodiscard]] const std::shared_ptr<SegmentData> &getData() const;
  [[maybe_unused]] [[nodiscard]] const std::chrono::system_clock::time_point &getLastUsedTimeStamp() const;
  [[maybe_unused]] [[nodiscard]] long getUsedCount() const;
  void setLastUsedTimeStamp(const std::chrono::time_point &LastUsedTimeStamp);
  void setUsedCount(long UsedCount);

private:
  std::shared_ptr<SegmentKey> key_;
  std::shared_ptr<SegmentData> data_;

  // Metrics to support algorithms like LRU, LFU, etc.
  std::chrono::system_clock::time_point lastUsedTimeStamp_;
  long usedCount_;

};

}

#endif //NORMAL_NORMAL_CACHE_INCLUDE_NORMAL_CACHE_SEGMENTCACHEENTRY_H
