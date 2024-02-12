//
// Created by Yifei Yang on 7/27/20.
//

#ifndef FPDB_FPDB_PLAN_INCLUDE_FPDB_PLAN_MODE_MODE_H
#define FPDB_FPDB_PLAN_INCLUDE_FPDB_PLAN_MODE_MODE_H

#include <fpdb/caf/CAFUtil.h>
#include <memory>

namespace fpdb::plan {

enum ModeId {
  PULL_UP,
  PUSHDOWN_ONLY,
  CACHING_ONLY,
  HYBRID
};

class Mode {
public:
  explicit Mode(ModeId modeId);
  Mode() = default;
  Mode(const Mode&) = default;
  Mode& operator=(const Mode&) = default;
  ~Mode() = default;

  ModeId id() const;
  bool is(const std::shared_ptr<Mode>& mode);
  std::string toString();

  static std::shared_ptr<Mode> pullupMode();
  static std::shared_ptr<Mode> pushdownOnlyMode();
  static std::shared_ptr<Mode> cachingOnlyMode();
  static std::shared_ptr<Mode> hybridMode();

private:
  ModeId id_;

// caf inspect
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, Mode& mode) {
    return f.object(mode).fields(f.field("id", mode.id_));
  }
};

}

using ModePtr = std::shared_ptr<fpdb::plan::Mode>;

CAF_BEGIN_TYPE_ID_BLOCK(Mode, fpdb::caf::CAFUtil::Mode_first_custom_type_id)
CAF_ADD_TYPE_ID(Mode, (fpdb::plan::Mode))
CAF_ADD_TYPE_ID(Mode, (ModePtr))      // needed to remote_spawn SegmentCacheActor
CAF_END_TYPE_ID_BLOCK(Mode)

namespace caf {
template <>
struct inspector_access<ModePtr> : variant_inspector_access<ModePtr> {
  // nop
};
}


#endif //FPDB_FPDB_PLAN_INCLUDE_FPDB_PLAN_MODE_MODE_H
