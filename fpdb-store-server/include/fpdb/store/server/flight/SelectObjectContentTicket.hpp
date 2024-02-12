//
// Created by Yifei Yang on 2/23/22.
//

#ifndef FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_FLIGHT_SELECTOBJECTCONTENTTICKET_HPP
#define FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_FLIGHT_SELECTOBJECTCONTENTTICKET_HPP

#include "fpdb/store/server/flight/TicketObject.hpp"
#include <nlohmann/json.hpp>

namespace fpdb::store::server::flight {

/**
 * Ticket for executing and returning the results of a query in the store.
 */
class SelectObjectContentTicket : public TicketObject {
public:
  SelectObjectContentTicket(long query_id,
                            const std::string &fpdb_store_super_pop,
                            const std::string &query_plan_string,
                            int parallel_degree);

  static std::shared_ptr<SelectObjectContentTicket> make(long query_id,
                                                         const std::string &fpdb_store_super_pop,
                                                         const std::string &query_plan_string,
                                                         int parallel_degree);

  long query_id() const;
  const std::string &fpdb_store_super_pop() const;
  const std::string &query_plan_string() const;
  int parallel_degree() const;

  tl::expected<std::string, std::string> serialize(bool pretty) override;

  static tl::expected<std::shared_ptr<SelectObjectContentTicket>, std::string> from_json(const nlohmann::json &jObj);

private:
  long query_id_;
  std::string fpdb_store_super_pop_;
  std::string query_plan_string_;
  int parallel_degree_;
};

}


#endif //FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_FLIGHT_SELECTOBJECTCONTENTTICKET_HPP
