//
// Created by Yifei Yang on 3/7/22.
//

#ifndef FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_FPDBSTORESERVERCONFIG_HPP
#define FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_FPDBSTORESERVERCONFIG_HPP

#include <string>
#include <memory>

namespace fpdb::store::server {

/**
 * fpdb-store-server config directly parsed from config file
 */
class FPDBStoreServerConfig {

public:
  FPDBStoreServerConfig(int fileServicePort,
                        int flightPort,
                        const std::string &storeRootPathPrefix,
                        int numDrives);

  static std::shared_ptr<FPDBStoreServerConfig> parseFPDBStoreServerConfig();

  int getFileServicePort() const;
  int getFlightPort() const;
  const std::string &getStoreRootPathPrefix() const;
  int getNumDrives() const;

private:
  int fileServicePort_;
  int flightPort_;
  std::string storeRootPathPrefix_;
  int numDrives_;

};

}


#endif //FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_FPDBSTORESERVERCONFIG_HPP
