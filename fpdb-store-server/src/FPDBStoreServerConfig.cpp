//
// Created by Yifei Yang on 3/7/22.
//

#include <fpdb/store/server/FPDBStoreServerConfig.hpp>
#include <fpdb/util/Util.h>
#include <unordered_map>

using namespace fpdb::util;

namespace fpdb::store::server {

FPDBStoreServerConfig::FPDBStoreServerConfig(int fileServicePort,
                                             int flightPort,
                                             const std::string &storeRootPathPrefix,
                                             int numDrives):
  fileServicePort_(fileServicePort),
  flightPort_(flightPort),
  storeRootPathPrefix_(storeRootPathPrefix),
  numDrives_(numDrives) {}

std::shared_ptr<FPDBStoreServerConfig> FPDBStoreServerConfig::parseFPDBStoreServerConfig() {
  std::unordered_map<std::string, std::string> configMap = readConfig("fpdb-store.conf");
  auto fileServicePort = std::stoi(configMap["FILE_SERVICE_PORT"]);
  auto flightPort = std::stoi(configMap["FLIGHT_PORT"]);
  auto storeRootPath = configMap["STORE_ROOT_PATH_PREFIX"];
  auto numDrives = std::stoi(configMap["NUM_DRIVES"]);
  return std::make_shared<FPDBStoreServerConfig>(fileServicePort, flightPort, storeRootPath, numDrives);
}

int FPDBStoreServerConfig::getFileServicePort() const {
  return fileServicePort_;
}

int FPDBStoreServerConfig::getFlightPort() const {
  return flightPort_;
}

const std::string &FPDBStoreServerConfig::getStoreRootPathPrefix() const {
  return storeRootPathPrefix_;
}

int FPDBStoreServerConfig::getNumDrives() const {
  return numDrives_;
}

}
