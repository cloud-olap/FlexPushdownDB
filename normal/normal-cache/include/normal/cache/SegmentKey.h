//
// Created by matt on 19/5/20.
//

#ifndef NORMAL_NORMAL_CACHE_INCLUDE_NORMAL_CACHE_SEGMENTKEY_H
#define NORMAL_NORMAL_CACHE_INCLUDE_NORMAL_CACHE_SEGMENTKEY_H

#include <memory>

#include <normal/connector/partition/Partition.h>

#include "SegmentRange.h"
#include "SegmentMetadata.h"

namespace normal::cache {

class SegmentKey {

public:
  SegmentKey(std::shared_ptr<Partition> Partition,
             std::string columnName,
             SegmentRange Range);

  SegmentKey(std::shared_ptr<Partition> Partition,
             std::string columnName,
             SegmentRange Range,
             std::shared_ptr<SegmentMetadata> metadata);

  static std::shared_ptr<SegmentKey> make(const std::shared_ptr<Partition> &Partition,
                                          std::string columnName,
                                          SegmentRange Range);

  static std::shared_ptr<SegmentKey> make(const std::shared_ptr<Partition> &Partition,
                                          std::string columnName,
                                          SegmentRange Range,
                                          std::shared_ptr<SegmentMetadata> metadata);

  [[nodiscard]] const std::shared_ptr<Partition> &getPartition() const;
  [[nodiscard]] const std::string &getColumnName() const;
  [[maybe_unused]] [[nodiscard]] const SegmentRange &getRange() const;
  [[nodiscard]] const std::shared_ptr<SegmentMetadata> &getMetadata() const;

  std::string toString();

  bool operator==(const SegmentKey& other) const;
  bool operator!=(const SegmentKey& other) const;

  size_t hash();

private:
  std::shared_ptr<Partition> partition_;
  std::string columnName_;
  SegmentRange range_;

  // FIXME: maybe not a good way to store metadata inside the SegmentKey
  std::shared_ptr<SegmentMetadata> metadata_;
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
