//
// Created by matt on 4/2/22.
//

#ifndef FPDB_FPDB_STORE_SERVER_TEST_GLOBAL_HPP
#define FPDB_FPDB_STORE_SERVER_TEST_GLOBAL_HPP

#include "fpdb/store/server/caf/ServerMeta.hpp"
#include "fpdb/store/server/caf/ActorManager.hpp"

#define BACKWARD_HAS_BFD 1
#include <backward.hpp>

#define LOG_LEVEL SPDLOG_LEVEL_DEBUG

#define SPDLOG_ACTIVE_LEVEL LOG_LEVEL
#include <spdlog/spdlog.h>

extern std::shared_ptr<fpdb::store::server::caf::ActorManager> actor_manager;

class Global {};

#endif // FPDB_FPDB_STORE_SERVER_TEST_GLOBAL_HPP
