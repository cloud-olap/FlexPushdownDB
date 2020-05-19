//
// Created by matt on 19/5/20.
//

#include "normal/cache/SegmentCacheEntry.h"

using namespace normal::cache;

SegmentCacheEntry::SegmentCacheEntry(std::shared_ptr<SegmentKey> Key,
									 std::shared_ptr<SegmentData> Data,
									 const std::chrono::system_clock::time_point &LastUsedTimeStamp,
									 long UsedCount) :
	key_(std::move(Key)),
	data_(std::move(Data)),
	lastUsedTimeStamp_(LastUsedTimeStamp),
	usedCount_(UsedCount) {}

std::shared_ptr<SegmentCacheEntry> SegmentCacheEntry::make(const std::shared_ptr<SegmentKey> &Key,
														   const std::shared_ptr<SegmentData> &Data) {
  return std::make_shared<SegmentCacheEntry>(Key, Data, std::chrono::system_clock::now(), 0);
}

const std::shared_ptr<SegmentKey> &SegmentCacheEntry::getKey() const {
  return key_;
}

const std::shared_ptr<SegmentData> &SegmentCacheEntry::getData() const {
  return data_;
}

[[maybe_unused]] const std::chrono::system_clock::time_point &SegmentCacheEntry::getLastUsedTimeStamp() const {
  return lastUsedTimeStamp_;
}

[[maybe_unused]] long SegmentCacheEntry::getUsedCount() const {
  return usedCount_;
}

void SegmentCacheEntry::setLastUsedTimeStamp(const std::chrono::time_point &LastUsedTimeStamp) {
  lastUsedTimeStamp_ = LastUsedTimeStamp;
}

void SegmentCacheEntry::setUsedCount(long UsedCount) {
  usedCount_ = UsedCount;
}

