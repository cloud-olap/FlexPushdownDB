//
// Created by Yifei Yang on 4/10/22.
//

#include "fpdb/store/server/flight/GetBitmapTicket.hpp"
#include "fpdb/store/server/flight/Util.hpp"
#include "fmt/format.h"

namespace fpdb::store::server::flight {

GetBitmapTicket::GetBitmapTicket(long query_id, const std::string &op):
  TicketObject(TicketType::get_bitmap()),
  query_id_(query_id),
  op_(op) {}

std::shared_ptr<GetBitmapTicket> GetBitmapTicket::make(long query_id, const std::string &op) {
  return std::make_shared<GetBitmapTicket>(query_id, op);
}

long GetBitmapTicket::query_id() const {
  return query_id_;
}

const std::string& GetBitmapTicket::op() const {
  return op_;
}

tl::expected<std::string, std::string> GetBitmapTicket::serialize(bool pretty) {
  nlohmann::json document;
  document.emplace(TypeJSONName.data(), type()->name());
  document.emplace(QueryIdJSONName.data(), query_id_);
  document.emplace(OpJSONName.data(), op_);
  return document.dump(pretty ? 2 : -1);
}

tl::expected<std::shared_ptr<GetBitmapTicket>, std::string> GetBitmapTicket::from_json(const nlohmann::json &jObj) {
  if (!jObj.contains(QueryIdJSONName.data())) {
    return tl::make_unexpected(fmt::format("Query id not specified in GetBitmapTicket JSON '{}'", to_string(jObj)));
  }
  auto query_id = jObj[QueryIdJSONName.data()].get<int64_t>();

  if (!jObj.contains(OpJSONName.data())) {
    return tl::make_unexpected(fmt::format("Op not specified in GetBitmapTicket JSON '{}'", to_string(jObj)));
  }
  auto op = jObj[OpJSONName.data()].get<std::string>();

  return GetBitmapTicket::make(query_id, op);
}

}
