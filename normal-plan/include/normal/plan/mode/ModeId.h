//
// Created by Yifei Yang on 7/27/20.
//

#ifndef NORMAL_NORMAL_PLAN_INCLUDE_NORMAL_PLAN_MODEID_H
#define NORMAL_NORMAL_PLAN_INCLUDE_NORMAL_PLAN_MODEID_H

namespace normal::plan::operator_::mode {

enum ModeId {
  HTAP,
  FullPullup,
  FullPushdown,
  PullupCaching,
  HybridCaching,
  HybridCachingLast
};

}


#endif //NORMAL_MODEID_H
