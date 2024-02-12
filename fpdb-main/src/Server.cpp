//
// Created by Yifei Yang on 1/14/22.
//

#include <fpdb/main/Server.h>
#include <fpdb/main/ExecConfig.h>
#include <fpdb/executor/caf/CAFInit.h>
#include <fpdb/executor/caf/CAFAdaptPushdownUtil.h>
#include <fpdb/executor/flight/FlightHandler.h>
#include <fpdb/executor/physical/Globals.h>
#include <fpdb/aws/AWSClient.h>
#include <fpdb/util/Util.h>
#include <iostream>
#include <future>
#include <utility>

using namespace fpdb::util;

namespace fpdb::main {

void Server::start() {
  // start the daemon AWS client
  const auto &awsConfig = AWSConfig::parseAWSConfig();
  fpdb::aws::AWSClient::daemonClient_ = make_shared<AWSClient>(awsConfig);
  fpdb::aws::AWSClient::daemonClient_->init();
  std::cout << "Daemon AWS client started" << std::endl;

  // start the daemon flight server if needed
  if (fpdb::executor::physical::USE_FLIGHT_COMM) {
    // local ip
    auto expLocalIp = getLocalIp();
    if (!expLocalIp.has_value()) {
      throw std::runtime_error(expLocalIp.error());
    }

    // make flight server
    ::arrow::flight::Location location;
    auto st = ::arrow::flight::Location::ForGrpcTcp("0.0.0.0", ExecConfig::parseFlightPort(), &location);
    if (!st.ok()) {
      throw std::runtime_error("Cannot open flight server: " + st.message());
    }
    fpdb::executor::flight::FlightHandler::daemonServer_ = std::make_unique<fpdb::executor::flight::FlightHandler>(
            location, *expLocalIp);

    // init
    auto res = fpdb::executor::flight::FlightHandler::daemonServer_->init();
    if (!res.has_value()) {
      throw std::runtime_error("Cannot open flight server: " + res.error());
    }

    // start
    flight_future_ = std::async(std::launch::async, [=]() {
      return fpdb::executor::flight::FlightHandler::daemonServer_->serve();
    });
    // Bit of a hack to check if the flight server failed on "serve"
    if (flight_future_.wait_for(100ms) == std::future_status::ready) {
      throw std::runtime_error("Cannot open flight server: " + flight_future_.get().error());
    }
    std::cout << "Daemon flight server started at port: "
              << fpdb::executor::flight::FlightHandler::daemonServer_->port()
              << std::endl;
  }

  // read remote Ips and server port
  const auto &remoteIps = readRemoteIps();
  int CAFServerPort = ExecConfig::parseCAFServerPort();

  // create the actor system
  actorSystemConfig_ = std::make_shared<ActorSystemConfig>(CAFServerPort, remoteIps, true);
  fpdb::executor::caf::CAFInit::initCAFGlobalMetaObjects();
  actorSystem_ = std::make_shared<::caf::actor_system>(*actorSystemConfig_);

  // open the port
  auto res = actorSystem_->middleman().open(actorSystemConfig_->port_);
  if (!res) {
    throw std::runtime_error("Cannot open CAF server at port: " + to_string(res.error()));
  } else {
    std::cout << "CAF server opened at port: " << actorSystemConfig_->port_ << std::endl;
  }

  // make actor system for adaptive pushdown if needed
  if (ENABLE_ADAPTIVE_PUSHDOWN) {
    fpdb::executor::caf::CAFAdaptPushdownUtil::startDaemonAdaptPushdownActorSystem();
    std::cout << "Daemon actor system for adaptive pushdown started" << std::endl;
  }

  std::cout << "Server started" << std::endl;
}

void Server::stop() {
  // stop the daemon AWS client
  fpdb::aws::AWSClient::daemonClient_->shutdown();
  std::cout << "Daemon AWS client stopped" << std::endl;

  // stop the daemon flight server if needed
  if (fpdb::executor::physical::USE_FLIGHT_COMM) {
    fpdb::executor::flight::FlightHandler::daemonServer_->shutdown();
    fpdb::executor::flight::FlightHandler::daemonServer_->wait();
    flight_future_.wait();
    fpdb::executor::flight::FlightHandler::daemonServer_.reset();
    std::cout << "Daemon flight server stopped" << std::endl;
  }

  // close the actor system
  if (actorSystem_) {
    auto res = actorSystem_->middleman().close(actorSystemConfig_->port_);
    if (!res) {
      throw std::runtime_error("Cannot close CAF server at port: " + to_string(res.error()));
    }
  }
  std::cout << "CAF server stopped" << std::endl;

  // stop actor system for adaptive pushdown if needed
  if (ENABLE_ADAPTIVE_PUSHDOWN) {
    fpdb::executor::caf::CAFAdaptPushdownUtil::stopDaemonAdaptPushdownActorSystem();
    std::cout << "Daemon actor system for adaptive pushdown stopped" << std::endl;
  }

  std::cout << "Server stopped" << std::endl;
}

}
