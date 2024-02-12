//
// Created by matt on 4/2/22.
//

#include "Global.hpp"

#include "fpdb/store/server/Server.hpp"
#include "fpdb/store/server/FPDBStoreServerConfig.hpp"

#include <csignal>

using namespace fpdb::store::server;
using namespace fpdb::store::server::flight;

std::shared_ptr<fpdb::store::server::Server> server;
std::shared_ptr<fpdb::store::server::caf::ActorManager> actorManager;

int main(int /*argc*/, char** /*argv*/) {
  auto fpdbStoreServerConfig = FPDBStoreServerConfig::parseFPDBStoreServerConfig();
  actorManager = fpdb::store::server::caf::ActorManager::make<::caf::id_block::Server>().value();
  server = fpdb::store::server::Server::make(ServerConfig{"node1",
                                                          10000,
                                                          true,
                                                          std::nullopt,
                                                          10001,
                                                          fpdbStoreServerConfig->getFlightPort(),
                                                          fpdbStoreServerConfig->getFileServicePort(),
                                                          fpdbStoreServerConfig->getStoreRootPathPrefix(),
                                                          fpdbStoreServerConfig->getNumDrives()},
                                             std::nullopt,
                                             actorManager);

  // Handle exit signals
  auto exitAct = [](int) {
    server->stop();
    server.reset();
    actorManager.reset();
    exit(0);
  };
  signal(SIGTERM, exitAct);
  signal(SIGINT, exitAct);
  signal(SIGABRT, exitAct);

  // Start the server
  auto init_result = server->init();
  if(!init_result.has_value()){
    SPDLOG_ERROR("Could not start Store Server, {}", init_result.error());
  }
  auto start_result = server->start();
  if(!start_result.has_value()){
    SPDLOG_ERROR("Could not start Store Server, {}", start_result.error());
  }

  server->join();

  return 0;
}