//
// Created by Yifei Yang on 2/21/22.
//

#include <fpdb/store/server/Server.hpp>
#include <fpdb/executor/physical/fpdb-store/FPDBStoreSuperPOp.h>
#include <fpdb/executor/physical/fpdb-store/FPDBStoreFileScanPOp.h>
#include <fpdb/executor/physical/filter/FilterPOp.h>
#include <fpdb/executor/physical/aggregate/AggregatePOp.h>
#include <fpdb/executor/physical/collate/CollatePOp.h>
#include <fpdb/executor/physical/aggregate/function/Sum.h>
#include <fpdb/executor/physical/serialization/PhysicalPlanSerializer.h>
#include <fpdb/expression/gandiva/LessThanOrEqualTo.h>
#include <fpdb/expression/gandiva/Column.h>
#include <fpdb/expression/gandiva/NumericLiteral.h>
#include <fpdb/tuple/csv/CSVFormat.h>
#include <fpdb/tuple/util/FileReaderTestUtil.h>
#include <doctest/doctest.h>
#include "Global.hpp"

using namespace fpdb::store::server;
using namespace fpdb::store::server::flight;
using namespace fpdb::executor::physical;
using namespace fpdb::expression::gandiva;
using namespace fpdb::tuple;

// file scan: test.csv, columns: {a, b}
std::shared_ptr<fpdb_store::FPDBStoreFileScanPOp> makeFPDBStoreFileScanPOp() {
  auto format = std::make_shared<csv::CSVFormat>(',');
  auto schema = fpdb::tuple::util::FileReaderTestUtil::makeTestSchema();
  return std::make_shared<fpdb_store::FPDBStoreFileScanPOp>("StoreFileScan",
                                                            std::vector<std::string>{"a", "b"},
                                                            0,
                                                            "test-resources",
                                                            "simple_data/csv/test.csv",
                                                            format,
                                                            schema);
}

// filter: a <= 4
std::shared_ptr<filter::FilterPOp> makeFilterPOp() {
  auto filterPredicate = lte(col("a"), num_lit<::arrow::Int64Type>(std::optional<long>(4)));
  return std::make_shared<filter::FilterPOp>("Filter",
                                             std::vector<std::string>{"b"},
                                             0,
                                             filterPredicate);
}

// aggregate: sum(b)
std::shared_ptr<aggregate::AggregatePOp> makeAggregatePOp() {
  std::vector<std::shared_ptr<aggregate::AggregateFunction>> functions{
          std::make_shared<aggregate::Sum>("sum_b", col("b"))};
  return std::make_shared<aggregate::AggregatePOp>("Aggregate",
                                                   std::vector<std::string>{"sum_b"},
                                                   0,
                                                   functions);
}

// collate
std::shared_ptr<fpdb::executor::physical::collate::CollatePOp>
makeCollatePOp(const std::vector<std::string> &projectColumnNames) {
  return std::make_shared<fpdb::executor::physical::collate::CollatePOp>("Collate",
                                                                         projectColumnNames,
                                                                         0);
}

// connect
void connect(const std::vector<std::pair<std::shared_ptr<PhysicalOp>, std::shared_ptr<PhysicalOp>>> &opPairs) {
  for (const auto &opPair: opPairs) {
    opPair.first->produce(opPair.second);
    opPair.second->consume(opPair.first);
  }
}

TEST_SUITE("fpdb-store-server/FlightSelectTest" * doctest::skip(false)) {

TEST_CASE("fpdb-store-server/FlightSelectTest/scan-filter-aggregate" * doctest::skip(false)) {

  // create store super op
  auto storeFileScanPOp = makeFPDBStoreFileScanPOp();
  auto filterPOp = makeFilterPOp();
  auto aggregatePOp = makeAggregatePOp();
  auto collatePOp = makeCollatePOp({"sum_b"});

  connect(std::vector<std::pair<std::shared_ptr<PhysicalOp>, std::shared_ptr<PhysicalOp>>>{
          {storeFileScanPOp, filterPOp},
          {filterPOp, aggregatePOp},
          {aggregatePOp, collatePOp}
  });

  // server
  ::arrow::Status st;
  auto server = fpdb::store::server::Server::make(ServerConfig{"1", 0, true, std::nullopt, 0, 0, 50051, "."}, std::nullopt, actor_manager);
  auto init_result = server->init();
  REQUIRE(init_result.has_value());
  auto start_result = server->start();
  REQUIRE(start_result.has_value());

  // client
  arrow::flight::Location client_location;
  auto port = server->flight_port();
  st = arrow::flight::Location::ForGrpcTcp("localhost", port, &client_location);
  REQUIRE(st.ok());
  arrow::flight::FlightClientOptions client_options = arrow::flight::FlightClientOptions::Defaults();
  std::unique_ptr<arrow::flight::FlightClient> client;
  st = arrow::flight::FlightClient::Connect(client_location, client_options, &client);
  REQUIRE(st.ok());

  // request
  auto subPlan = std::make_shared<PhysicalPlan>(std::vector<std::shared_ptr<PhysicalOp>>{
    storeFileScanPOp, filterPOp, aggregatePOp, collatePOp});
  PhysicalPlanSerializer planSerializer(subPlan);
  auto expPlanString = planSerializer.serialize(false);
  REQUIRE(expPlanString.has_value());
  auto ticketObj = SelectObjectContentTicket::make(*expPlanString);
  auto expTicket = ticketObj->to_ticket(false);
  REQUIRE(expTicket.has_value());

  std::unique_ptr<::arrow::flight::FlightStreamReader> reader;
  st = client->DoGet(*expTicket, &reader);
  REQUIRE(st.ok());

  std::shared_ptr<::arrow::Table> table;
  st = reader->ReadAll(&table);
  REQUIRE(st.ok());

  // check
  auto tupleSet = TupleSet::make(table);
  REQUIRE_EQ(tupleSet->numRows(), 1);
  REQUIRE_EQ(tupleSet->numColumns(), 1);
  auto expValue = tupleSet->value<::arrow::Int64Type>("sum_b", 0);
  REQUIRE(expValue.has_value());
  REQUIRE_EQ(*expValue, 7);

  // stop
  server->stop();
}

}
