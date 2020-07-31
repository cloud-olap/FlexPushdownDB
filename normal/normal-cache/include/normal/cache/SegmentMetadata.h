//
// Created by Yifei Yang on 7/30/20.
//

#ifndef NORMAL_NORMAL_CACHE_INCLUDE_NORMAL_CACHE_SEGMENTMETADATA_H
#define NORMAL_NORMAL_CACHE_INCLUDE_NORMAL_CACHE_SEGMENTMETADATA_H

#include <memory>

namespace normal::cache {

class SegmentMetadata {

public:
  SegmentMetadata(size_t size);

  static std::shared_ptr<SegmentMetadata> make(size_t size);

  size_t size() const;

private:
  size_t size_;

};

}


#endif //NORMAL_NORMAL_CACHE_INCLUDE_NORMAL_CACHE_SEGMENTMETADATA_H
