//
// Created by Yifei Yang on 4/18/22.
//

#include <fpdb/executor/Globals.h>
#include <fpdb/executor/flight/FlightClients.h>
#include <fpdb/executor/physical/Globals.h>

namespace fpdb::executor {

void clearGlobal() {
  flight::GlobalFlightClients.reset();
}

bool enableAdaptExec() {
  return DIST_PRED_TRANS_TYPE == DistPredTransType::ADAPT || DIST_JOIN_TYPE == join::DistJoinType::COST_BASED_ADAPT;
}

}
