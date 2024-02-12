//
// Created by matt on 11/2/22.
//

#include <doctest/doctest.h>

#include "Global.hpp"

#include "fpdb/store/server/Server.hpp"

using namespace fpdb::store::server;
using namespace fpdb::store::server::flight;
using namespace fpdb::store::server::caf;

TEST_SUITE("fpdb-store-server/DistributedTest" * doctest::skip(false)) {

  TEST_CASE("fpdb-store-server/DistributedTest/boot" * doctest::skip(false)) {

    // Start server 1
    auto server1 = fpdb::store::server::Server::make(ServerConfig{"node1", 0, true, std::nullopt, 0,  0, 50051, "./cluster/node1/data"}, std::nullopt,
                                actor_manager);
    auto init_result1 = server1->init();
    REQUIRE(init_result1.has_value());
    auto start_result1 = server1->start();
    REQUIRE(start_result1.has_value());

    // Start server 2
    auto server2 = fpdb::store::server::Server::make(ServerConfig{"node2", 0, false, std::nullopt, std::nullopt, 0, 50051, "./cluster/node2/data"},
                                server1->cluster_actor_handle(), actor_manager);
    auto init_result2 = server2->init();
    REQUIRE(init_result2.has_value());
    auto start_result2 = server2->start();
    REQUIRE(start_result2.has_value());

    server1->stop();
    server2->stop();
  }
}