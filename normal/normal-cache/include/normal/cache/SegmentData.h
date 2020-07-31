//
// Created by matt on 19/5/20.
//

#ifndef NORMAL_NORMAL_CACHE_INCLUDE_NORMAL_CACHE_SEGMENTDATA_H
#define NORMAL_NORMAL_CACHE_INCLUDE_NORMAL_CACHE_SEGMENTDATA_H

#include <memory>

#include <normal/tuple/TupleSet2.h>

using namespace normal::tuple;

namespace normal::cache {

class SegmentData {

public:
  explicit SegmentData(std::shared_ptr<Column> column);

  static std::shared_ptr<SegmentData> make(const std::shared_ptr<Column> &column);
  [[nodiscard]] const std::shared_ptr<Column> &getColumn() const;

private:
  std::shared_ptr<Column> column_;

};

}

#endif //NORMAL_NORMAL_CACHE_INCLUDE_NORMAL_CACHE_SEGMENTDATA_H
