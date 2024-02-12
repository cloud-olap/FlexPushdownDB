//
// Created by matt on 4/2/22.
//

#ifndef FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_FLIGHT_TICKETOBJECT_HPP
#define FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_FLIGHT_TICKETOBJECT_HPP

#include <memory>

#include <arrow/flight/api.h>
#include <tl/expected.hpp>

#include "TicketType.hpp"

namespace fpdb::store::server::flight {

/**
 * A structured representation of a Flight ticket. Concrete subclasses of type GetObject and SelectObjectContent ticket.
 *
 */
class TicketObject {
public:
  explicit TicketObject(std::shared_ptr<TicketType> type);

  virtual ~TicketObject() = default;

  virtual tl::expected<std::string, std::string> serialize(bool pretty) = 0;

  static tl::expected<std::shared_ptr<TicketObject>, std::string> deserialize(const std::string& ticket_string);

  static tl::expected<std::shared_ptr<TicketObject>, std::string> deserialize(const ::arrow::flight::Ticket& ticket);

  tl::expected<::arrow::flight::Ticket, std::string> to_ticket(bool pretty);

  [[nodiscard]] const std::shared_ptr<TicketType>& type() const;

private:
  std::shared_ptr<TicketType> type_;
};

} // namespace fpdb::store::server::flight

#endif // FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_FLIGHT_TICKETOBJECT_HPP
