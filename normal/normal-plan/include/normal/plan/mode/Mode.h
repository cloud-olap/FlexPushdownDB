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

  Mode(ModeId modeId);
  ~Mode() = default;

  bool is(std::shared_ptr<Mode> mode);
  std::string toString();

  ModeId id() const;

private:
  ModeId id_;
};

}


#endif //NORMAL_MODE_H
