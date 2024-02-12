//
// Created by matt on 19/5/20.
//

#ifndef FPDB_FPDB_CACHE_INCLUDE_FPDB_CACHE_SEGMENTDATA_H
#define FPDB_FPDB_CACHE_INCLUDE_FPDB_CACHE_SEGMENTDATA_H

#include <fpdb/tuple/Column.h>
#include <fpdb/caf/CAFUtil.h>
#include <memory>

using namespace fpdb::tuple;

namespace fpdb::cache {

class SegmentData {

public:
  explicit SegmentData(std::shared_ptr<Column> column);
  SegmentData() = default;
  SegmentData(const SegmentData&) = default;
  SegmentData& operator=(const SegmentData&) = default;

  static std::shared_ptr<SegmentData> make(const std::shared_ptr<Column> &column);
  [[nodiscard]] const std::shared_ptr<Column> &getColumn() const;

private:
  std::shared_ptr<Column> column_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, SegmentData& segmentData) {
    return f.apply(segmentData.column_);
  };
};

}

using SegmentDataPtr = std::shared_ptr<fpdb::cache::SegmentData>;

CAF_BEGIN_TYPE_ID_BLOCK(SegmentData, fpdb::caf::CAFUtil::SegmentData_first_custom_type_id)
CAF_ADD_TYPE_ID(SegmentData, (fpdb::cache::SegmentData))
CAF_END_TYPE_ID_BLOCK(SegmentData)

namespace caf {
template <>
struct inspector_access<SegmentDataPtr> : variant_inspector_access<SegmentDataPtr> {
  // nop
};
} // namespace caf

#endif //FPDB_FPDB_CACHE_INCLUDE_FPDB_CACHE_SEGMENTDATA_H
