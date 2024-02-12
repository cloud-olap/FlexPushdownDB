//
// Created by Yifei Yang on 7/30/20.
//

#ifndef FPDB_FPDB_CACHE_INCLUDE_FPDB_CACHE_SEGMENTMETADATA_H
#define FPDB_FPDB_CACHE_INCLUDE_FPDB_CACHE_SEGMENTMETADATA_H

#include <fpdb/caf/CAFUtil.h>
#include <memory>

namespace fpdb::cache {

class SegmentMetadata {

public:
  SegmentMetadata(size_t size);
  SegmentMetadata(const SegmentMetadata &m);
  SegmentMetadata() = default;
  SegmentMetadata& operator=(const SegmentMetadata&) = default;

  static std::shared_ptr<SegmentMetadata> make(size_t size);

  [[nodiscard]] size_t size() const;
  [[nodiscard]] int hitNum() const;
  [[nodiscard]] double perSizeFreq() const;
  [[nodiscard]] double value() const;
  [[nodiscard]] bool valid() const;

  void setSize(size_t size);
  void incHitNum();
  void incHitNum(size_t size);
  void addValue(double value);
  void invalidate();

private:
  size_t size_;
  int hitNum_;
  double perSizeFreq_;
  double value_;
  bool valid_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, SegmentMetadata& m) {
    return f.object(m).fields(f.field("size", m.size_),
                              f.field("hitNum", m.hitNum_),
                              f.field("perSizeFreq", m.perSizeFreq_),
                              f.field("value", m.value_),
                              f.field("valid", m.valid_));
  }
};

}

CAF_BEGIN_TYPE_ID_BLOCK(SegmentMetadata, fpdb::caf::CAFUtil::SegmentMetadata_first_custom_type_id)
CAF_ADD_TYPE_ID(SegmentMetadata, (fpdb::cache::SegmentMetadata))
CAF_END_TYPE_ID_BLOCK(SegmentMetadata)

using SegmentMetadataPtr = std::shared_ptr<fpdb::cache::SegmentMetadata>;

namespace caf {
template <>
struct inspector_access<SegmentMetadataPtr> : variant_inspector_access<SegmentMetadataPtr> {
  // nop
};

} // namespace caf

#endif //FPDB_FPDB_CACHE_INCLUDE_FPDB_CACHE_SEGMENTMETADATA_H
