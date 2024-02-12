//
// Created by Yifei Yang on 4/18/22.
//

#include "fpdb/store/server/flight/ClearBitmapCmd.hpp"
#include "fpdb/store/server/flight/Util.hpp"
#include "fmt/format.h"

namespace fpdb::store::server::flight {

ClearBitmapCmd::ClearBitmapCmd(BitmapType bitmap_type, long query_id, const std::string &op):
  CmdObject(CmdType::clear_bitmap()),
  bitmap_type_(bitmap_type),
  query_id_(query_id),
  op_(op) {}

std::shared_ptr<ClearBitmapCmd> ClearBitmapCmd::make(BitmapType bitmap_type, long query_id, const std::string &op) {
  return std::make_shared<ClearBitmapCmd>(bitmap_type, query_id, op);
}

BitmapType ClearBitmapCmd::bitmap_type() const {
  return bitmap_type_;
}

long ClearBitmapCmd::query_id() const {
  return query_id_;
}

const std::string& ClearBitmapCmd::op() const {
  return op_;
}

tl::expected<std::string, std::string> ClearBitmapCmd::serialize(bool pretty) {
  nlohmann::json value;
  value.emplace(TypeJSONName.data(), type()->name());
  value.emplace(BitmapTypeJSONName.data(), bitmap_type_);
  value.emplace(QueryIdJSONName.data(), query_id_);
  value.emplace(OpJSONName.data(), op_);
  return value.dump(pretty ? 2 : -1);
}

tl::expected<std::shared_ptr<ClearBitmapCmd>, std::string> ClearBitmapCmd::from_json(const nlohmann::json& jObj) {
  if (!jObj.contains(BitmapTypeJSONName.data())) {
    return tl::make_unexpected(fmt::format("Bitmap type name not specified in ClearBitmapCmd JSON '{}'", to_string(jObj)));
  }
  auto bitmap_type = jObj[BitmapTypeJSONName.data()].get<BitmapType>();

  if (!jObj.contains(QueryIdJSONName.data())) {
    return tl::make_unexpected(fmt::format("Query id not specified in ClearBitmapCmd JSON '{}'", to_string(jObj)));
  }
  auto query_id = jObj[QueryIdJSONName.data()].get<long>();

  if (!jObj.contains(OpJSONName.data())) {
    return tl::make_unexpected(fmt::format("Op not specified in ClearBitmapCmd JSON '{}'", to_string(jObj)));
  }
  auto op = jObj[OpJSONName.data()].get<std::string>();

  return ClearBitmapCmd::make(bitmap_type, query_id, op);
}

}
