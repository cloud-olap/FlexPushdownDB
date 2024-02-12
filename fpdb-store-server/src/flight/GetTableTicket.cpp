//
// Created by Yifei Yang on 7/4/22.
//

#include "fpdb/store/server/flight/GetTableTicket.hpp"
#include "fpdb/store/server/flight/Util.hpp"
#include "fmt/format.h"

namespace fpdb::store::server::flight {

GetTableTicket::GetTableTicket(long query_id,
                               const std::string &producer,
                               const std::string &consumer,
                               bool wait_not_exist):
  TicketObject(TicketType::get_table()),
  query_id_(query_id),
  producer_(producer),
  consumer_(consumer),
  wait_not_exist_(wait_not_exist) {}

std::shared_ptr<GetTableTicket>
GetTableTicket::make(long query_id,
                     const std::string &producer,
                     const std::string &consumer,
                     bool wait_not_exist) {
  return std::make_shared<GetTableTicket>(query_id, producer, consumer, wait_not_exist);
}

long GetTableTicket::query_id() const {
  return query_id_;
}

const std::string& GetTableTicket::producer() const {
  return producer_;
}

const std::string& GetTableTicket::consumer() const {
  return consumer_;
}

bool GetTableTicket::wait_not_exist() const {
  return wait_not_exist_;
}

tl::expected<std::string, std::string> GetTableTicket::serialize(bool pretty) {
  nlohmann::json document;
  document.emplace(TypeJSONName.data(), type()->name());
  document.emplace(QueryIdJSONName.data(), query_id_);
  document.emplace(ProducerJSONName.data(), producer_);
  document.emplace(ConsumerJSONName.data(), consumer_);
  document.emplace(WaitNotExistJSONName.data(), wait_not_exist_);
  return document.dump(pretty ? 2 : -1);
}

tl::expected<std::shared_ptr<GetTableTicket>, std::string> GetTableTicket::from_json(const nlohmann::json &jObj) {
  if (!jObj.contains(QueryIdJSONName.data())) {
    return tl::make_unexpected(fmt::format("Query id not specified in GetTableTicket JSON '{}'", to_string(jObj)));
  }
  auto query_id = jObj[QueryIdJSONName.data()].get<int64_t>();

  if (!jObj.contains(ProducerJSONName.data())) {
    return tl::make_unexpected(fmt::format("Producer not specified in GetTableTicket JSON '{}'", to_string(jObj)));
  }
  auto producer = jObj[ProducerJSONName.data()].get<std::string>();

  if (!jObj.contains(ConsumerJSONName.data())) {
    return tl::make_unexpected(fmt::format("Consumer not specified in GetTableTicket JSON '{}'", to_string(jObj)));
  }
  auto consumer = jObj[ConsumerJSONName.data()].get<std::string>();

  if (!jObj.contains(WaitNotExistJSONName)) {
    return tl::make_unexpected(fmt::format("WaitNotExist not specified in GetTableTicket JSON '{}'", to_string(jObj)));
  }
  auto wait_not_exist = jObj[WaitNotExistJSONName.data()].get<bool>();

  return GetTableTicket::make(query_id, producer, consumer, wait_not_exist);
}

}
