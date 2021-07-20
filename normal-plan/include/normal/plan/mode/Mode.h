//
// Created by Yifei Yang on 7/27/20.
//

#ifndef NORMAL_NORMAL_PLAN_INCLUDE_NORMAL_PLAN_MODE_H
#define NORMAL_NORMAL_PLAN_INCLUDE_NORMAL_PLAN_MODE_H

#include <memory>
#include "ModeId.h"

namespace normal::plan::operator_::mode {

class Mode {
public:

  explicit Mode(ModeId modeId);
  ~Mode() = default;

  bool is(const std::shared_ptr<Mode>& mode);
  std::string toString();

  [[nodiscard]] ModeId id() const;

private:
  ModeId id_;
};

}


#endif //NORMAL_NORMAL_PLAN_INCLUDE_NORMAL_PLAN_MODE_H
