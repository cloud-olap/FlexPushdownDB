//
// Created by matt on 16/2/22.
//

#ifndef FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_SERVERCONFIG_HPP
#define FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_SERVERCONFIG_HPP

#include <string>
#include <optional>

namespace fpdb::store::server {

/**
 * fpdb-store-server config used by server
 */
class ServerConfig {
public:
  std::string name;
  int node_port;

  bool start_coordinator;
  std::optional<std::string> coordinator_host;
  std::optional<int> coordinator_port;

  int flight_port;
  int file_service_port;

  std::string store_root_path_prefix_;
  int num_drives_;
};

} // namespace fpdb::store::server

#endif // FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_SERVERCONFIG_HPP
