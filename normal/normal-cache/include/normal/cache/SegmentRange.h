//
// Created by matt on 19/5/20.
//

#ifndef NORMAL_NORMAL_CACHE_INCLUDE_NORMAL_CACHE_SEGMENTRANGE_H
#define NORMAL_NORMAL_CACHE_INCLUDE_NORMAL_CACHE_SEGMENTRANGE_H

#include <string>

namespace normal::cache {

class SegmentRange {

public:
  SegmentRange(unsigned long StartOffset, unsigned long FinishOffset);

  static SegmentRange make(unsigned long StartOffset, unsigned long FinishOffset);

  std::string toString();

  bool operator==(const SegmentRange& other) const;

  [[nodiscard]] size_t hash() const;

private:
  unsigned long startOffset_;
  unsigned long finishOffset_;

};

}

#endif //NORMAL_NORMAL_CACHE_INCLUDE_NORMAL_CACHE_SEGMENTRANGE_H
