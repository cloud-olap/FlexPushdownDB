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
  static std::shared_ptr<SegmentMetadata> make();
  static std::shared_ptr<SegmentMetadata> make(size_t estimateSize, size_t size);

  size_t size() const;
  int hitNum() const;
  size_t estimateSize() const;

  void setSize(size_t size);

  void incHitNum();

private:
  size_t estimateSize_;
  size_t size_;
  int hitNum_;

};

}


#endif //NORMAL_NORMAL_CACHE_INCLUDE_NORMAL_CACHE_SEGMENTMETADATA_H
