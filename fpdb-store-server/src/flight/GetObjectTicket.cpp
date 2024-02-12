//
// Created by Yifei Yang on 2/23/22.
//

#include "fpdb/store/server/flight/GetObjectTicket.hpp"
#include "fpdb/store/server/flight/Util.hpp"
#include "fmt/format.h"

namespace fpdb::store::server::flight {


GetObjectTicket::GetObjectTicket(std::string bucket, std::string object)
        : TicketObject(TicketType::get_object()), bucket_(std::move(bucket)), object_(std::move(object)) {
}

std::shared_ptr<GetObjectTicket> GetObjectTicket::make(std::string bucket, std::string object) {
  return std::make_shared<GetObjectTicket>(std::move(bucket), std::move(object));
}

const std::string& GetObjectTicket::object() const {
  return object_;
}

const std::string& GetObjectTicket::bucket() const {
  return bucket_;
}

tl::expected<std::string, std::string> GetObjectTicket::serialize(bool pretty) {
  nlohmann::json document;
  document.emplace(TypeJSONName.data(), type()->name());
  document.emplace(BucketJSONName.data(), bucket_.c_str());
  document.emplace(ObjectJSONName.data(), object_.c_str());
  return document.dump(pretty ? 2 : -1);
}

tl::expected<std::shared_ptr<GetObjectTicket>, std::string> GetObjectTicket::from_json(const nlohmann::json &jObj) {
  if (!jObj.contains(BucketJSONName.data())) {
    return tl::make_unexpected(fmt::format("Bucket not specified in GetObjectTicket JSON '{}'", to_string(jObj)));
  }
  auto bucket = jObj[BucketJSONName.data()].get<std::string>();

  if (!jObj.contains(ObjectJSONName.data())) {
    return tl::make_unexpected(fmt::format("Object not specified in GetObjectTicket JSON '{}'", to_string(jObj)));
  }
  auto object = jObj[ObjectJSONName.data()].get<std::string>();

  return GetObjectTicket::make(bucket, object);
}

}
