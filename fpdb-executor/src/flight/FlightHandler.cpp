//
// Created by Yifei Yang on 9/28/22.
//

#include <fpdb/executor/flight/FlightHandler.h>
#include <fpdb/tuple/util/Util.h>
#include <fmt/format.h>

using namespace fpdb::store::server::flight;

namespace fpdb::executor::flight {

FlightHandler::FlightHandler(Location location, const std::string &host):
  location_(std::move(location)),
  host_(host) {}

FlightHandler::~FlightHandler() {
  this->shutdown();
}

const std::string &FlightHandler::getHost() const {
  return host_;
}

int FlightHandler::getPort() const {
  return FlightServerBase::port();
}

tl::expected<void, std::string> FlightHandler::init() {
  FlightServerOptions options(location_);
  auto st = this->Init(options);
  if (!st.ok()) {
    return tl::make_unexpected(st.message());
  }
  return {};
}

tl::expected<void, std::string> FlightHandler::serve() {
  auto st = this->Serve();
  if (!st.ok()) {
    return tl::make_unexpected(st.message());
  }
  return {};
}

tl::expected<void, std::string> FlightHandler::shutdown() {
  auto st = this->Shutdown();
  if (!st.ok()) {
    return tl::make_unexpected(st.message());
  }
  return {};
}

tl::expected<void, std::string> FlightHandler::wait() {
  auto st = this->Wait();
  if (!st.ok()) {
    return tl::make_unexpected(st.message());
  }
  return {};
}

void FlightHandler::putTable(long queryId, const std::string &producer, const std::string &consumer,
                             const std::shared_ptr<arrow::Table> &table) {
  table_cache_.produceTable(cache::TableCache::generateTableKey(queryId, producer, consumer), table);
}

::arrow::Status FlightHandler::DoGet(const ServerCallContext& context, const Ticket& request,
                                     std::unique_ptr<FlightDataStream>* stream) {
  auto expected_flight_stream = do_get(context, request);
  if (!expected_flight_stream.has_value()) {
    return expected_flight_stream.error();
  }
  *stream = std::move(expected_flight_stream.value());
  return ::arrow::Status::OK();
}

tl::expected<std::unique_ptr<FlightDataStream>, ::arrow::Status> FlightHandler::do_get(const ServerCallContext& context,
                                                                                       const Ticket& request) {

  auto expected_ticket_object = TicketObject::deserialize(request);
  if (!expected_ticket_object.has_value()) {
    return tl::make_unexpected(MakeFlightError(FlightStatusCode::Failed, expected_ticket_object.error()));
  }
  auto ticket_object = expected_ticket_object.value();

  switch(ticket_object->type()->id()) {
    case TicketTypeId::GET_TABLE: {
      auto get_table_ticket = std::static_pointer_cast<GetTableTicket>(ticket_object);
      return do_get_get_table(context, get_table_ticket);
    }
    default: {
      return tl::make_unexpected(MakeFlightError(FlightStatusCode::Failed,
              fmt::format("Unrecognized Flight Ticket type '{}' for compute cluster", ticket_object->type()->name())));
    }
  }
}

tl::expected<std::unique_ptr<FlightDataStream>, ::arrow::Status> FlightHandler::do_get_get_table(
        const ServerCallContext&, const std::shared_ptr<GetTableTicket>& get_table_ticket) {
  // get table from table cache
  auto query_id = get_table_ticket->query_id();
  auto producer = get_table_ticket->producer();
  auto consumer = get_table_ticket->consumer();
  auto table_key = cache::TableCache::generateTableKey(query_id, producer, consumer);
  auto exp_table = table_cache_.consumeTable(table_key);
  if (!exp_table.has_value()) {
    return tl::make_unexpected(MakeFlightError(FlightStatusCode::Failed, exp_table.error()));
  }

  // make record batch stream and return
  auto exp_batches = tuple::util::Util::table_to_record_batches(*exp_table);
  if (!exp_batches.has_value()) {
    return tl::make_unexpected(MakeFlightError(FlightStatusCode::Failed, exp_batches.error()));
  }
  auto rb_reader = ::arrow::RecordBatchReader::Make(*exp_batches);
  return std::make_unique<::arrow::flight::RecordBatchStream>(*rb_reader);
}

}
