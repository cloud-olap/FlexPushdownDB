//
// Created by Yifei Yang on 11/29/22.
//

#ifndef FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_FLIGHT_GETBATCHLOADINFOTICKET_HPP
#define FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_FLIGHT_GETBATCHLOADINFOTICKET_HPP

#include "fpdb/store/server/flight/TicketObject.hpp"
#include <nlohmann/json.hpp>

namespace fpdb::store::server::flight {

/**
 * Ticket for batch loading table for multiple consumers constructed from store, e.g. shuffle results.
 */
class GetBatchLoadInfoTicket : public TicketObject {

public:
  GetBatchLoadInfoTicket(long query_id,
                         const std::string &producer,
                         const std::vector<std::string> &consumers,
                         const std::string &batch_load_pop);

  static std::shared_ptr<GetBatchLoadInfoTicket> make(long query_id,
                                                      const std::string &producer,
                                                      const std::vector<std::string> &consumers,
                                                      const std::string &batch_load_pop);

  long query_id() const;
  const std::string &producer() const;
  const std::vector<std::string> &consumers() const;
  const std::string &batch_load_pop() const;

  tl::expected<std::string, std::string> serialize(bool pretty) override;

  static tl::expected<std::shared_ptr<GetBatchLoadInfoTicket>, std::string> from_json(const nlohmann::json &jObj);

private:
  long query_id_;
  std::string producer_;
  std::vector<std::string> consumers_;
  std::string batch_load_pop_;
};

}


#endif //FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_FLIGHT_GETBATCHLOADINFOTICKET_HPP
