//
// Created by Yifei Yang on 9/14/22.
//

#include <fpdb/executor/physical/fpdb-store/FPDBStoreBloomFilterInfo.h>
#include <fmt/format.h>

namespace fpdb::executor::physical::fpdb_store {

FPDBStoreBloomFilterUseInfo::FPDBStoreBloomFilterUseInfo(
        const std::string bloomFilterCreatePOp, const std::vector<std::string> &columnNames,
        const std::optional<std::shared_ptr<bloomfilter::BloomFilterBase>>& bloomFilter):
  bloomFilterCreatePOp_(bloomFilterCreatePOp),
  columnNames_(columnNames),
  bloomFilter_(bloomFilter) {}

::nlohmann::json FPDBStoreBloomFilterUseInfo::toJson() const {
  ::nlohmann::json jObj;
  jObj.emplace("bloomFilterCreatePOp", bloomFilterCreatePOp_);
  jObj.emplace("columnNames", columnNames_);
  return jObj;
}

tl::expected<std::shared_ptr<FPDBStoreBloomFilterUseInfo>, std::string>
FPDBStoreBloomFilterUseInfo::fromJson(const ::nlohmann::json &jObj) {
  if (!jObj.contains("bloomFilterCreatePOp")) {
    return tl::make_unexpected(
            fmt::format("BloomFilterCreatePOp not specified in FPDBStoreBloomFilterUseInfo JSON '{}'", to_string(jObj)));
  }
  auto bloomFilterCreatePOp = jObj["bloomFilterCreatePOp"].get<std::string>();

  if (!jObj.contains("columnNames")) {
    return tl::make_unexpected(
            fmt::format("ColumnNames not specified in FPDBStoreBloomFilterUseInfo JSON '{}'", to_string(jObj)));
  }
  auto columnNames = jObj["columnNames"].get<std::vector<std::string>>();

  return std::make_shared<FPDBStoreBloomFilterUseInfo>(bloomFilterCreatePOp, columnNames);
}

}
