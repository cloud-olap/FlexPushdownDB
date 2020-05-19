//
// Created by matt on 19/5/20.
//

#ifndef NORMAL_NORMAL_CACHE_INCLUDE_NORMAL_CACHE_SEGMENTKEY_H
#define NORMAL_NORMAL_CACHE_INCLUDE_NORMAL_CACHE_SEGMENTKEY_H

#include <memory>

#include <normal/connector/partition/Partition.h>

#include "SegmentRange.h"

namespace normal::cache {

class SegmentKey {

public:
  SegmentKey(std::shared_ptr<Partition> Partition, SegmentRange Range);

  static std::shared_ptr<SegmentKey> make(const std::shared_ptr<Partition> &Partition, SegmentRange Range);

  [[nodiscard]] const std::shared_ptr<Partition> &getPartition() const;
  [[maybe_unused]] [[nodiscard]] const SegmentRange &getRange() const;

  std::string toString();

  bool operator==(const SegmentKey& other) const;
  bool operator!=(const SegmentKey& other) const;

  size_t hash();

private:
  std::shared_ptr<Partition> partition_;
  SegmentRange range_;

};

struct SegmentKeyPointerHash {
  inline size_t operator()(const std::shared_ptr<SegmentKey> &key) const {
	return key->hash();
  }
};

struct SegmentKeyPointerPredicate {
  inline bool operator()(const std::shared_ptr<SegmentKey>& lhs, const std::shared_ptr<SegmentKey>& rhs) const {
	return *lhs == *rhs;
  }
};

}

#endif //NORMAL_NORMAL_CACHE_INCLUDE_NORMAL_CACHE_SEGMENTKEY_H
