//
// Created by matt on 4/2/22.
//

#include <fmt/format.h>
#include <nlohmann/json.hpp>

#include "fpdb/store/server/flight/GetObjectTicket.hpp"
#include "fpdb/store/server/flight/SelectObjectContentTicket.hpp"
#include "fpdb/store/server/flight/GetBitmapTicket.hpp"
#include "fpdb/store/server/flight/GetTableTicket.hpp"
#include "fpdb/store/server/flight/GetBatchLoadInfoTicket.hpp"
#include "fpdb/store/server/flight/Util.hpp"

namespace fpdb::store::server::flight {

TicketObject::TicketObject(std::shared_ptr<TicketType> type) : type_(std::move(type)) {
}

const std::shared_ptr<TicketType>& TicketObject::type() const {
  return type_;
}

tl::expected<::arrow::flight::Ticket, std::string> TicketObject::to_ticket(bool pretty) {
  auto serializeRes = serialize(pretty);
  if (!serializeRes.has_value()) {
    return tl::make_unexpected(serializeRes.error());
  }
  return arrow::flight::Ticket{*serializeRes};
}

tl::expected<std::shared_ptr<TicketObject>, std::string> TicketObject::deserialize(const std::string& ticket_string) {
  auto document = nlohmann::json::parse(ticket_string);
  if (!document.is_object()) {
    return tl::make_unexpected(fmt::format("Cannot parse Ticket object JSON '{}'", ticket_string));
  }

  if (!document.contains(TypeJSONName.data())) {
    return tl::make_unexpected(fmt::format("Type not specified in Ticket object JSON '{}'", ticket_string));
  }
  auto type = document[TypeJSONName.data()].get<std::string>();

  if (type == GetObjectTicketTypeName.data()) {
    return GetObjectTicket::from_json(document);
  } else if (type == SelectObjectContentTicketTypeName.data()) {
    return SelectObjectContentTicket::from_json(document);
  } else if (type == GetBitmapTicketTypeName.data()) {
    return GetBitmapTicket::from_json(document);
  } else if (type == GetTableTicketTypeName.data()) {
    return GetTableTicket::from_json(document);
  } else if (type == GetBatchLoadInfoTicketTypeName.data()) {
    return GetBatchLoadInfoTicket::from_json(document);
  } else {
    return tl::make_unexpected(fmt::format("Unsupported Ticket object type: '{}'", type));
  }
}

tl::expected<std::shared_ptr<TicketObject>, std::string>
TicketObject::deserialize(const ::arrow::flight::Ticket& ticket) {
  return deserialize(ticket.ticket);
}

} // namespace fpdb::store::server::flight