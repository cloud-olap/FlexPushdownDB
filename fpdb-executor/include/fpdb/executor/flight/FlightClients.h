//
// Created by Yifei Yang on 11/17/22.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_FLIGHT_FLIGHTCLIENTS_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_FLIGHT_FLIGHTCLIENTS_H

#include <arrow/flight/api.h>
#include <unordered_map>
#include <string>
#include <shared_mutex>

namespace fpdb::executor::flight {

/**
 * Make a single shared client for each destination
 */
class FlightClients {

public:
  arrow::flight::FlightClient* getFlightClient(const std::string &host, int port);
  void reset();

private:
  static std::string generateFlightClientKey(const std::string &host, int port);

  std::unordered_map<std::string, std::unique_ptr<arrow::flight::FlightClient>> clients_;
  std::shared_mutex mutex_;
};

inline FlightClients GlobalFlightClients;

};


#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_FLIGHT_FLIGHTCLIENTS_H
