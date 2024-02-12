//
// Created by Yifei Yang on 2/23/22.
//

#ifndef FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_FLIGHT_GETOBJECTTICKET_HPP
#define FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_FLIGHT_GETOBJECTTICKET_HPP

#include "fpdb/store/server/flight/TicketObject.hpp"
#include <nlohmann/json.hpp>

namespace fpdb::store::server::flight {

/**
 * Ticket for retrieving a whole object from the store.
 */
class GetObjectTicket : public TicketObject {
public:
  GetObjectTicket(std::string bucket, std::string object);

  static std::shared_ptr<GetObjectTicket> make(std::string bucket, std::string object);

  [[nodiscard]] const std::string& bucket() const;

  [[nodiscard]] const std::string& object() const;

  tl::expected<std::string, std::string> serialize(bool pretty) override;

  static tl::expected<std::shared_ptr<GetObjectTicket>, std::string> from_json(const nlohmann::json &jObj);

private:
  std::string bucket_;
  std::string object_;
};

}


#endif //FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_FLIGHT_GETOBJECTTICKET_HPP
