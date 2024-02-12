//
// Created by matt on 4/2/22.
//

#include <future>

#include <doctest/doctest.h>
#include <fpdb/store/server/Server.hpp>
#include <fpdb/store/server/flight/FlightHandler.hpp>

#include "Global.hpp"

using namespace fpdb::store::server;
using namespace fpdb::store::server::flight;

TEST_SUITE("fpdb-store-server/FlightBasicTest" * doctest::skip(false)) {

  TEST_CASE("fpdb-store-server/FlightBasicTest/test-cmd-serialization" * doctest::skip(false)) {

    // Define cmd
    auto src_cmd = SelectObjectContentCmd::make("DUMMY_PLAN");

    // Serialize cmd
    auto exp_src_string = src_cmd->serialize(true);
    REQUIRE(exp_src_string.has_value());
    auto src_string = *exp_src_string;

    SPDLOG_DEBUG(src_string);
    REQUIRE_EQ(src_string, R"({
  "query_plan": "DUMMY_PLAN",
  "type": "select-object-content"
})");

    // Deserialize cmd
    auto exp_dst_cmd = CmdObject::deserialize(src_string);
    REQUIRE(exp_dst_cmd.has_value());
    auto dst_cmd = *exp_dst_cmd;

    auto exp_dst_string = dst_cmd->serialize(true);
    REQUIRE(exp_dst_string.has_value());
    auto dst_string = *exp_dst_string;

    SPDLOG_DEBUG(dst_string);
    REQUIRE_EQ(dst_string, R"({
  "query_plan": "DUMMY_PLAN",
  "type": "select-object-content"
})");
  }

  TEST_CASE("fpdb-store-server/FlightBasicTest/test-ticket-serialization" * doctest::skip(false)) {

    // Define ticket
    auto src_ticket = SelectObjectContentTicket::make("DUMMY_PLAN");

    // Serialize ticket
    auto exp_src_string = src_ticket->serialize(true);
    REQUIRE(exp_src_string.has_value());
    auto src_string = *exp_src_string;

    SPDLOG_DEBUG(src_string);
    REQUIRE_EQ(src_string, R"({
  "query_plan": "DUMMY_PLAN",
  "type": "select-object-content"
})");

    // Deserialize ticket
    auto exp_dst_ticket = TicketObject::deserialize(src_string);
    REQUIRE(exp_dst_ticket.has_value());
    auto dst_ticket = *exp_dst_ticket;

    auto exp_dst_string = dst_ticket->serialize(true);
    REQUIRE(exp_dst_string.has_value());
    auto dst_string = *exp_dst_string;

    SPDLOG_DEBUG(dst_string);
    REQUIRE_EQ(dst_string, R"({
  "query_plan": "DUMMY_PLAN",
  "type": "select-object-content"
})");
  }

  TEST_CASE("fpdb-store-server/FlightBasicTest/test-get-flight-info" * doctest::skip(false)) {

    ::arrow::Status st;

    // Start the server
    auto server = fpdb::store::server::Server::make(ServerConfig{"1", 0, true, std::nullopt, 0, 0, 50051, "./single/data"}, std::nullopt,
                               actor_manager);
    auto init_result = server->init();
    REQUIRE(init_result.has_value());
    auto start_result = server->start();
    REQUIRE(start_result.has_value());

    // Connect the client
    arrow::flight::Location client_location;
    auto port = server->flight_port();
    st = arrow::flight::Location::ForGrpcTcp("localhost", port, &client_location);
    REQUIRE(st.ok());
    arrow::flight::FlightClientOptions client_options = arrow::flight::FlightClientOptions::Defaults();
    std::unique_ptr<arrow::flight::FlightClient> client;
    st = arrow::flight::FlightClient::Connect(client_location, client_options, &client);
    REQUIRE(st.ok());

    // Get FlightInfo
    arrow::flight::FlightCallOptions call_options;
    call_options.headers.emplace_back("bucket", "ssb-sf0.01");
    call_options.headers.emplace_back("object", "customer.csv");
    auto flight_descriptor = FlightDescriptor::Path({});
    std::unique_ptr<FlightInfo> flight_info;
    st = client->GetFlightInfo(call_options, flight_descriptor, &flight_info);
    REQUIRE(st.ok());

    SPDLOG_DEBUG(flight_info->descriptor().ToString());
    SPDLOG_DEBUG(flight_info->endpoints()[0].ticket.ticket);
    SPDLOG_DEBUG(flight_info->endpoints()[0].locations.size());

    server->stop();
  }

  TEST_CASE("fpdb-store-server/FlightBasicTest/test-do-get-get-object" * doctest::skip(false)) {

    ::arrow::Status st;

    auto server = fpdb::store::server::Server::make(ServerConfig{"1", 0, true, std::nullopt, 0, 0, 50051, "./single/data"}, std::nullopt,
                               actor_manager);
    auto init_result = server->init();
    REQUIRE(init_result.has_value());
    auto start_result = server->start();
    REQUIRE(start_result.has_value());

    // Connect the client
    arrow::flight::Location client_location;
    auto port = server->flight_port();
    st = arrow::flight::Location::ForGrpcTcp("localhost", port, &client_location);
    REQUIRE(st.ok());
    arrow::flight::FlightClientOptions client_options = arrow::flight::FlightClientOptions::Defaults();
    std::unique_ptr<arrow::flight::FlightClient> client;
    st = arrow::flight::FlightClient::Connect(client_location, client_options, &client);
    REQUIRE(st.ok());

    auto ticket_obj = GetObjectTicket::make("ssb-sf0.01", "customer.csv");
    auto exp_ticket = ticket_obj->to_ticket(false);
    REQUIRE(exp_ticket.has_value());

    std::unique_ptr<::arrow::flight::FlightStreamReader> reader;
    st = client->DoGet(*exp_ticket, &reader);
    REQUIRE(st.ok());

    std::shared_ptr<::arrow::Table> table;
    st = reader->ReadAll(&table);
    REQUIRE(st.ok());

    auto options = ::arrow::PrettyPrintOptions::Defaults();
    options.skip_new_lines = true;
    st = ::arrow::PrettyPrint(*table, options, &std::cout);
    REQUIRE(st.ok());

    REQUIRE_EQ(table->num_columns(), 3);
    REQUIRE_EQ(table->num_rows(), 20);

    server->stop();
  }
}