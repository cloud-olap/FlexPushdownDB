//
// Created by Yifei Yang on 4/18/22.
//

#include <fpdb/executor/Globals.h>
#include <fpdb/executor/flight/FlightClients.h>

namespace fpdb::executor {

void clearGlobal() {
  flight::GlobalFlightClients.reset();
}

}
