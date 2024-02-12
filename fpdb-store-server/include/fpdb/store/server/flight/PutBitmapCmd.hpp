//
// Created by Yifei Yang on 4/17/22.
//

#ifndef FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_FLIGHT_PUTBITMAPCMD_HPP
#define FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_FLIGHT_PUTBITMAPCMD_HPP

#include "fpdb/store/server/flight/CmdObject.hpp"
#include "fpdb/store/server/flight/BitmapType.h"

namespace fpdb::store::server::flight {

class PutBitmapCmd : public CmdObject {

public:
  PutBitmapCmd(BitmapType bitmap_type, long query_id, const std::string &op, bool valid,
               const std::optional<int> &num_copies = std::nullopt,
               const std::optional<nlohmann::json> &bloom_filter = std::nullopt);

  static std::shared_ptr<PutBitmapCmd>
  make(BitmapType bitmap_type, long query_id, const std::string &op, bool valid,
       const std::optional<int> &num_copies = std::nullopt,
       const std::optional<nlohmann::json> &bloom_filter = std::nullopt);

  BitmapType bitmap_type() const;
  long query_id() const;
  const std::string& op() const;
  bool valid() const;
  const std::optional<int> &num_copies() const;
  const std::optional<nlohmann::json> &bloom_filter() const;

  tl::expected<std::string, std::string> serialize(bool pretty) override;
  static tl::expected<std::shared_ptr<PutBitmapCmd>, std::string> from_json(const nlohmann::json& jObj);

private:
  BitmapType bitmap_type_;
  long query_id_;
  std::string op_;
  bool valid_;

  // set when pushing down bloom filter bitmap to store, bloom_filter_ here is serialized json obj without bitmap
  std::optional<int> num_copies_;
  std::optional<nlohmann::json> bloom_filter_;

};

}


#endif //FPDB_FPDB_STORE_SERVER_INCLUDE_FPDB_STORE_SERVER_FLIGHT_PUTBITMAPCMD_HPP
