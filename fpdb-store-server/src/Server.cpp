//
// Created by matt on 10/2/22.
//

#include "fpdb/store/server/Server.hpp"
#include "fpdb/store/server/Global.hpp"
#include "fpdb/store/server/caf/ServerMeta.hpp"
#include "fpdb/store/server/cluster/ClusterActor.hpp"
#include "fpdb/store/server/cluster/NodeActor.hpp"
#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <grpcpp/health_check_service_interface.h>
#include <caf/io/all.hpp>
#include <future>
#include <utility>

namespace fpdb::store::server {

using namespace std::chrono_literals;
using namespace fpdb::store::server::cluster;

Server::Server(const ServerConfig& cfg, std::optional<ClusterActor> remote_coordinator_actor_handle,
               std::shared_ptr<caf::ActorManager> actor_manager)
    : name_(cfg.name),
      node_port_(cfg.node_port),
      start_coordinator_(cfg.start_coordinator),
      coordinator_host_(cfg.coordinator_host),
      coordinator_port_(cfg.coordinator_port),
      flight_port_(cfg.flight_port),
      file_service_port_(cfg.file_service_port),
      store_root_path_prefix_(cfg.store_root_path_prefix_),
      num_drives_(cfg.num_drives_),
      actor_manager_(std::move(actor_manager)),
      cluster_actor_handle_(std::move(remote_coordinator_actor_handle)) {
}

Server::~Server() {
  if(running_) {
    stop();
  }
}

std::shared_ptr<Server> Server::make(const ServerConfig& cfg, const std::optional<ClusterActor>& coordinator_actor_handle,
                                     std::optional<std::shared_ptr<caf::ActorManager>> optional_actor_manager) {

  std::shared_ptr<caf::ActorManager> actor_manager;
  if(optional_actor_manager) {
    actor_manager = optional_actor_manager.value();
  } else {
    actor_manager = caf::ActorManager::make<::caf::id_block::Server>().value();
  }

  return std::make_shared<Server>(cfg, coordinator_actor_handle, actor_manager);
}

tl::expected<void, std::string> Server::init() {

  ::arrow::Status st;

  signal_handler_ = std::make_unique<SignalHandler>([&](int signum) {
    SPDLOG_INFO("{}  |  Received signal (Signal {}:{})", name_, signum, strsignal(signum));
    stop_except_signal_handler();
  });
  signal_handler_->start();

  // Init the flight handler
  ::arrow::flight::Location server_location;
  st = ::arrow::flight::Location::ForGrpcTcp("0.0.0.0", flight_port_, &server_location);
  if(!st.ok()) {
    return tl::make_unexpected(fmt::format("Could not start FlightHandler, {}", st.message()));
  }
  flight_handler_ = std::make_unique<FlightHandler>(server_location,
                                                    actor_manager_->actor_system(),
                                                    store_root_path_prefix_,
                                                    num_drives_);
  auto ex = flight_handler_->init();
  if(!ex.has_value()) {
    return tl::make_unexpected(fmt::format("Could not start FlightHandler, {}", st.message()));
  }

  flight_port_ = flight_handler_->port();

  return {};
}

tl::expected<void, std::string> Server::start() {

  assert(!running_);

  SPDLOG_INFO("FlexPushdownDB Store Server ({}) starting", name_);

  if(start_coordinator_) {
    // Local coordinator, start coordinator and node actor
    cluster_actor_handle_ = actor_manager_->actor_system()->spawn(ClusterActorState::actor_functor);

    auto expected_coordinator_port = actor_manager_->actor_system()->middleman().publish(cluster_actor_handle_.value(),
                                                                                        coordinator_port_.value());
    coordinator_port_ = expected_coordinator_port.value();

    SPDLOG_DEBUG("Started local Coordinator actor (port: {})", coordinator_port_.value());

    node_actor_handle_ = actor_manager_->actor_system()->spawn(NodeActorState::actor_functor, cluster_actor_handle_,
                                                              std::nullopt, std::nullopt);
    auto expected_node_port = actor_manager_->actor_system()->middleman().publish(node_actor_handle_, node_port_);
    node_port_ = expected_node_port.value();

    SPDLOG_DEBUG("Started Node actor (port: {})", node_port_);
  } else {
    // Remote coordinator, just start node actor
    if(cluster_actor_handle_.has_value()) {
      node_actor_handle_ = actor_manager_->actor_system()->spawn(NodeActorState::actor_functor,
                                                                cluster_actor_handle_.value(), std::nullopt,
                                                                std::nullopt);
    } else {
      node_actor_handle_ = actor_manager_->actor_system()->spawn(NodeActorState::actor_functor, std::nullopt,
                                                                coordinator_host_, coordinator_port_);
    }

    auto expected_node_port = actor_manager_->actor_system()->middleman().publish(node_actor_handle_, node_port_);
    node_port_ = expected_node_port.value();

    SPDLOG_DEBUG("Started Node actor (port: {})", node_port_);
  }

  flight_future_ = std::async(std::launch::async, [=]() { return flight_handler_->serve(); });

  // Start the file service
  file_service_ = std::make_unique<file::FileServiceHandler>(store_root_path_prefix_, num_drives_);

  grpc::EnableDefaultHealthCheckService(true);
  grpc::reflection::InitProtoReflectionServerBuilderPlugin();
  grpc::ServerBuilder builder;

  builder.AddListeningPort("0.0.0.0:" + std::to_string(file_service_port_), grpc::InsecureServerCredentials());
  builder.RegisterService(&*file_service_);

  file_service_server_ = builder.BuildAndStart();
  file_service_future_ = std::async(std::launch::async, [=]() { return file_service_server_->Wait(); });

  // Bit of a hack to check if the flight server failed on "serve"
  if(flight_future_.wait_for(100ms) == std::future_status::ready) {
    return tl::make_unexpected(flight_future_.get().error());
  }

  running_ = true;

  SPDLOG_INFO("FlexPushdownDB Store Server ({}) started (FlightHandler listening on {})", name_, flight_port_);

  return {};
}

void Server::stop() {

  assert(running_);

  SPDLOG_INFO("FlexPushdownDB Store Server ({}) stopping", name_);

  ::caf::scoped_actor self(*actor_manager_->actor_system());
  self->send_exit(node_actor_handle_, ::caf::exit_reason::user_shutdown);
  if(start_coordinator_) {
    self->send_exit(cluster_actor_handle_.value(), ::caf::exit_reason::user_shutdown);
  }

  flight_handler_->shutdown();
  flight_handler_->wait();
  flight_future_.wait();

  file_service_server_->Shutdown();
  file_service_future_.wait();

  signal_handler_->stop();

  running_ = false;

  SPDLOG_INFO("FlexPushdownDB Store Server ({}) stopped ", name_);
}

void Server::join() {
  flight_handler_->wait();
  flight_future_.wait();
}

int Server::flight_port() const {
  return flight_port_;
}

bool Server::running() const {
  return running_;
}

void Server::stop_except_signal_handler() {

  assert(running_);

  SPDLOG_INFO("FlexPushdownDB Store Server stopping");

  flight_handler_->shutdown();
  flight_handler_->wait();
  flight_future_.wait();

  running_ = false;

  SPDLOG_INFO("FlexPushdownDB Store Server stopped ");
}

const std::optional<int>& Server::coordinator_port() const {
  return coordinator_port_;
}

const std::optional<ClusterActor>& Server::cluster_actor_handle() const {
  return cluster_actor_handle_;
}

} // namespace fpdb::store::server
