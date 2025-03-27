//
// Created by Yifei Yang on 11/17/22.
//

#include <fpdb/executor/flight/FlightClients.h>

namespace fpdb::executor::flight {

arrow::flight::FlightClient* FlightClients::getFlightClient(const std::string &host, int port) {
  std::unique_lock lock(mutex_);

  // check if already made before
  std::string key = generateFlightClientKey(host, port);
  auto clientIt = clients_.find(key);
  if (clientIt != clients_.end()) {
    return clientIt->second.get();
  }

  // if not made yet, make one and save
  auto expClientLocation = arrow::flight::Location::ForGrpcTcp(host, port);
  if (!expClientLocation.ok()) {
    throw std::runtime_error(expClientLocation.status().message());
  }

  arrow::flight::FlightClientOptions clientOptions = arrow::flight::FlightClientOptions::Defaults();
  auto expClient = arrow::flight::FlightClient::Connect(*expClientLocation, clientOptions);
  if (!expClient.ok()) {
    throw std::runtime_error(expClient.status().message());
  }

  auto clientRawPtr = (*expClient).get();
  clients_[key] = std::move(*expClient);
  return clientRawPtr;
}

void FlightClients::reset() {
  clients_.clear();
}

std::string FlightClients::generateFlightClientKey(const std::string &host, int port) {
  return host + ":" + std::to_string(port);
}

}
