//
// Created by Yifei Yang on 7/30/20.
//

#ifndef NORMAL_NORMAL_CACHE_INCLUDE_NORMAL_CACHE_SEGMENTMETADATA_H
#define NORMAL_NORMAL_CACHE_INCLUDE_NORMAL_CACHE_SEGMENTMETADATA_H

#include <memory>

namespace normal::cache {

class SegmentMetadata {

public:
  SegmentMetadata();
  SegmentMetadata(size_t estimateSize, size_t size);
  SegmentMetadata(const SegmentMetadata &m);
  static std::shared_ptr<SegmentMetadata> make();
  static std::shared_ptr<SegmentMetadata> make(size_t estimateSize, size_t size);

  void setSize(size_t size);
  [[nodiscard]] size_t size() const;
  [[nodiscard]] int hitNum() const;
  [[nodiscard]] size_t estimateSize() const;
  [[nodiscard]] double perSizeFreq() const;

  [[nodiscard]] double value() const;
  [[nodiscard]] double avgValue() const;
  [[nodiscard]] double value2() const;
  [[nodiscard]] bool valid() const;

  void incHitNum();
  void incHitNum(size_t size);
  void addValue(double value);
  void invalidate();

private:
  size_t estimateSize_;
  size_t size_;
  int hitNum_;
  double perSizeFreq_;
  double value_;
  bool valid_;
};

}


#endif //NORMAL_NORMAL_CACHE_INCLUDE_NORMAL_CACHE_SEGMENTMETADATA_H
