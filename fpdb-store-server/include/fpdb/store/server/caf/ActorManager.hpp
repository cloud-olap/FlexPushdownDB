//
// Created by matt on 11/2/22.
//

#ifndef FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_CAF_ACTORMANAGER_HPP
#define FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_CAF_ACTORMANAGER_HPP

#include <optional>

#include <caf/all.hpp>
#include <caf/io/all.hpp>
#include <tl/expected.hpp>

namespace fpdb::store::server::caf {

class ActorManager {
public:
  ActorManager();

  template<typename... T>
  static tl::expected<std::shared_ptr<ActorManager>, std::string>
  make(std::optional<size_t> max_threads = std::nullopt) {

    auto actor_manager = std::make_shared<ActorManager>();

    // Setting this to a level lower than info only works for a debug build of CAF
    actor_manager->actor_system_config_->set("caf.logger.console.verbosity", "info");

    auto processor_count = std::thread::hardware_concurrency();
    actor_manager->actor_system_config_->set(
      "caf.scheduler.max-threads", max_threads.value_or(std::max(1U, processor_count - DefaultReservedCores)));

    // Initialize global type information
    ::caf::init_global_meta_objects<T...>();
    ::caf::core::init_global_meta_objects();
    ::caf::io::middleman::init_global_meta_objects();
    // Load modules
    actor_manager->actor_system_config_->load<::caf::io::middleman>();
    // Create actor system
    actor_manager->actor_system_ = std::make_shared<::caf::actor_system>(*actor_manager->actor_system_config_);
    actor_manager->actor_system_->await_actors_before_shutdown(true);

    return actor_manager;
  }

  [[nodiscard]] const std::shared_ptr<::caf::actor_system>& actor_system() const;

private:
  inline static constexpr uint DefaultReservedCores = 0;

  std::shared_ptr<::caf::actor_system_config> actor_system_config_;
  std::shared_ptr<::caf::actor_system> actor_system_;
};

} // namespace fpdb::store::server::caf

#endif // FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_CAF_ACTORMANAGER_HPP
