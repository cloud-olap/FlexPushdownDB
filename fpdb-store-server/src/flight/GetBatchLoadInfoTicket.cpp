//
// Created by Yifei Yang on 11/29/22.
//

#include "fpdb/store/server/flight/GetBatchLoadInfoTicket.hpp"
#include "fpdb/store/server/flight/Util.hpp"
#include "fmt/format.h"

namespace fpdb::store::server::flight {

GetBatchLoadInfoTicket::GetBatchLoadInfoTicket(long query_id,
                                               const std::string &producer,
                                               const std::vector<std::string> &consumers,
                                               const std::string &batch_load_pop):
  TicketObject(TicketType::get_batch_load_info()),
  query_id_(query_id),
  producer_(producer),
  consumers_(consumers),
  batch_load_pop_(batch_load_pop) {}

std::shared_ptr<GetBatchLoadInfoTicket>
GetBatchLoadInfoTicket::make(long query_id,
                             const std::string &producer,
                             const std::vector<std::string> &consumers,
                             const std::string &batch_load_pop) {
  return std::make_shared<GetBatchLoadInfoTicket>(query_id, producer, consumers, batch_load_pop);
}

long GetBatchLoadInfoTicket::query_id() const {
  return query_id_;
}

const std::string &GetBatchLoadInfoTicket::producer() const {
  return producer_;
}

const std::vector<std::string> &GetBatchLoadInfoTicket::consumers() const {
  return consumers_;
}

const std::string &GetBatchLoadInfoTicket::batch_load_pop() const {
  return batch_load_pop_;
}

tl::expected<std::string, std::string> GetBatchLoadInfoTicket::serialize(bool pretty) {
  nlohmann::json document;
  document.emplace(TypeJSONName.data(), type()->name());
  document.emplace(QueryIdJSONName.data(), query_id_);
  document.emplace(ProducerJSONName.data(), producer_);
  document.emplace(ConsumersJSONName.data(), consumers_);
  document.emplace(BatchLoadPOpJSONName.data(), batch_load_pop_);
  return document.dump(pretty ? 2 : -1);
}

tl::expected<std::shared_ptr<GetBatchLoadInfoTicket>, std::string>
GetBatchLoadInfoTicket::from_json(const nlohmann::json &jObj) {
  if (!jObj.contains(QueryIdJSONName.data())) {
    return tl::make_unexpected(fmt::format("Query id not specified in GetBatchLoadInfoTicket JSON '{}'", to_string(jObj)));
  }
  auto query_id = jObj[QueryIdJSONName.data()].get<int64_t>();

  if (!jObj.contains(ProducerJSONName.data())) {
    return tl::make_unexpected(fmt::format("Producer not specified in GetBatchLoadInfoTicket JSON '{}'", to_string(jObj)));
  }
  auto producer = jObj[ProducerJSONName.data()].get<std::string>();

  if (!jObj.contains(ConsumersJSONName.data())) {
    return tl::make_unexpected(fmt::format("Consumers not specified in GetBatchLoadInfoTicket JSON '{}'", to_string(jObj)));
  }
  auto consumers = jObj[ConsumersJSONName.data()].get<std::vector<std::string>>();

  if (!jObj.contains(BatchLoadPOpJSONName.data())) {
    return tl::make_unexpected(fmt::format("batch_load_pop not specified in GetBatchLoadInfoTicket JSON '{}'", to_string(jObj)));
  }
  auto batch_load_pop = jObj[BatchLoadPOpJSONName.data()].get<std::string>();

  return GetBatchLoadInfoTicket::make(query_id, producer, consumers, batch_load_pop);
}

}
