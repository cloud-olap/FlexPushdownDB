//
// Created by matt on 10/2/22.
//

#ifndef FPDB_FPDB_STORE_SERVER_SRC_GLOBAL_HPP
#define FPDB_FPDB_STORE_SERVER_SRC_GLOBAL_HPP

#define LOG_LEVEL SPDLOG_LEVEL_DEBUG

#define SPDLOG_ACTIVE_LEVEL LOG_LEVEL
#include <spdlog/spdlog.h>

namespace fpdb::store::server {

static constexpr uint16_t FPDB_STORE_FIRST_CAF_TYPE_ID = ::caf::first_custom_type_id + 10000;

}


#endif // FPDB_FPDB_STORE_SERVER_SRC_GLOBAL_HPP
