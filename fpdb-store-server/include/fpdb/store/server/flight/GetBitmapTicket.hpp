//
// Created by Yifei Yang on 4/10/22.
//

#ifndef FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_FLIGHT_GETBITMAPTICKET_HPP
#define FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_FLIGHT_GETBITMAPTICKET_HPP

#include "fpdb/store/server/flight/TicketObject.hpp"
#include <nlohmann/json.hpp>
#include <optional>

namespace fpdb::store::server::flight {

/**
 * Ticket for fetching bitmap constructed from store, used in bitmap pushdown.
 */
class GetBitmapTicket : public TicketObject {

public:
  GetBitmapTicket(long query_id, const std::string &op);

  static std::shared_ptr<GetBitmapTicket> make(long query_id, const std::string &op);

  long query_id() const;
  const std::string& op() const;
  const std::optional<std::string> &consumer() const;
  void set_consumer(const std::string &consumer);

  tl::expected<std::string, std::string> serialize(bool pretty) override;

  static tl::expected<std::shared_ptr<GetBitmapTicket>, std::string> from_json(const nlohmann::json &jObj);

private:
  long query_id_;
  std::string op_;    // producer of the bitmap
  std::optional<std::string> consumer_ = std::nullopt;   // used when the bitmap is specific to the consumer

};

}


#endif //FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_FLIGHT_GETBITMAPTICKET_HPP
