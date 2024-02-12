//
// Created by Yifei Yang on 9/28/22.
//

#ifndef FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_FLIGHT_FLIGHTHANDLER_H
#define FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_FLIGHT_FLIGHTHANDLER_H

#include <fpdb/executor/cache/TableCache.h>
#include <fpdb/store/server/flight/GetTableTicket.hpp>
#include <arrow/api.h>
#include <arrow/flight/api.h>
#include <tl/expected.hpp>

using namespace ::arrow::flight;

namespace fpdb::executor::flight {

class FlightHandler : public FlightServerBase {

public:
  // A global flight server used for slave nodes in distributed node.
  inline static std::unique_ptr<FlightHandler> daemonServer_ = nullptr;

  FlightHandler(Location location, const std::string &host);
  ~FlightHandler() override;

  const std::string &getHost() const;
  int getPort() const;

  tl::expected<void, std::string> init();
  tl::expected<void, std::string> serve();
  tl::expected<void, std::string> shutdown();
  tl::expected<void, std::string> wait();

  void putTable(long queryId, const std::string &producer, const std::string &consumer,
                const std::shared_ptr<arrow::Table> &table);

  ::arrow::Status DoGet(const ServerCallContext& context, const Ticket& request,
                        std::unique_ptr<FlightDataStream>* stream) override;

private:
  tl::expected<std::unique_ptr<FlightDataStream>, ::arrow::Status> do_get(const ServerCallContext& context,
                                                                          const Ticket& request);

  tl::expected<std::unique_ptr<FlightDataStream>, ::arrow::Status>
  do_get_get_table(const ServerCallContext& context,
                   const std::shared_ptr<store::server::flight::GetTableTicket>& get_table_ticket);

  Location location_;
  std::string host_;
  cache::TableCache table_cache_;

};

}


#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_FLIGHT_FLIGHTHANDLER_H
