//
// Created by Yifei Yang on 3/7/22.
//

#include <fpdb/store/client/FPDBStoreClientConfig.h>
#include <fpdb/util/Util.h>
#include <unordered_map>

using namespace fpdb::util;

namespace fpdb::store::client {

FPDBStoreClientConfig::FPDBStoreClientConfig(const std::vector<std::string> &hosts,
                                             int fileServicePort,
                                             int flightPort):
  hosts_(hosts),
  fileServicePort_(fileServicePort),
  flightPort_(flightPort) {}

std::shared_ptr<FPDBStoreClientConfig> FPDBStoreClientConfig::parseFPDBStoreClientConfig() {
  std::unordered_map<string, string> configMap = readConfig("fpdb-store.conf");
  auto fileServicePort = std::stoi(configMap["FILE_SERVICE_PORT"]);
  auto flightPort = std::stoi(configMap["FLIGHT_PORT"]);
  const auto &hosts = readRemoteIps(false);
  return std::make_shared<FPDBStoreClientConfig>(hosts, fileServicePort, flightPort);
}

const std::vector<std::string> &FPDBStoreClientConfig::getHosts() const {
  return hosts_;
}

int FPDBStoreClientConfig::getFileServicePort() const {
  return fileServicePort_;
}

int FPDBStoreClientConfig::getFlightPort() const {
  return flightPort_;
}

}
