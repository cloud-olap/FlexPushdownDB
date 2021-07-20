//
// Created by Yifei Yang on 8/15/20.
//

#ifndef NORMAL_NORMAL_PLAN_INCLUDE_NORMAL_PLAN_HYBRIDCACHINGLASTMODE_H
#define NORMAL_NORMAL_PLAN_INCLUDE_NORMAL_PLAN_HYBRIDCACHINGLASTMODE_H

#include "Mode.h"

namespace normal::plan::operator_::mode {

/**
 * Hybrid caching using the last layout
 */
class HybridCachingLastMode : public Mode {

public:
  HybridCachingLastMode();
};

}

#endif //NORMAL_NORMAL_PLAN_INCLUDE_NORMAL_PLAN_HYBRIDCACHINGLASTMODE_H
