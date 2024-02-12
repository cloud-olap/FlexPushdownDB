//
// Created by Yifei Yang on 4/10/22.
//

#include <fpdb/executor/physical/fpdb-store/FPDBStoreFilterBitmapWrapper.h>
#include <fmt/format.h>

namespace fpdb::executor::physical::fpdb_store {

::nlohmann::json FPDBStoreFilterBitmapWrapper::toJson() const {
  ::nlohmann::json jObj;

  jObj.emplace("fpdbStoreSuperPOp", fpdbStoreSuperPOp_);
  jObj.emplace("mirrorOp", mirrorOp_);
  jObj.emplace("isComputeSide", isComputeSide_);
  jObj.emplace("isBitmapSent", isBitmapSent_);

  // bitmap_ is sent using arrow flight for performance
  // host_ and port_ are not needed in storage side

  return jObj;
}

tl::expected<FPDBStoreFilterBitmapWrapper, std::string> FPDBStoreFilterBitmapWrapper::fromJson(const ::nlohmann::json &jObj) {
  FPDBStoreFilterBitmapWrapper fpdbStoreFilterBitmapWrapper{};

  if (!jObj.contains("fpdbStoreSuperPOp")) {
    return tl::make_unexpected(fmt::format("FpdbStoreSuperPOp not specified in FPDBStoreFilterBitmapWrapper JSON '{}'", to_string(jObj)));
  }
  fpdbStoreFilterBitmapWrapper.fpdbStoreSuperPOp_ = jObj["fpdbStoreSuperPOp"].get<std::string>();

  if (!jObj.contains("mirrorOp")) {
    return tl::make_unexpected(fmt::format("MirrorOp not specified in FPDBStoreFilterBitmapWrapper JSON '{}'", to_string(jObj)));
  }
  fpdbStoreFilterBitmapWrapper.mirrorOp_ = jObj["mirrorOp"].get<std::string>();

  if (!jObj.contains("isComputeSide")) {
    return tl::make_unexpected(fmt::format("IsComputeSide not specified in FPDBStoreFilterBitmapWrapper JSON '{}'", to_string(jObj)));
  }
  fpdbStoreFilterBitmapWrapper.isComputeSide_ = jObj["isComputeSide"].get<bool>();

  if (!jObj.contains("isBitmapSent")) {
    return tl::make_unexpected(fmt::format("IsBitmapSent not specified in FPDBStoreFilterBitmapWrapper JSON '{}'", to_string(jObj)));
  }
  fpdbStoreFilterBitmapWrapper.isBitmapSent_ = jObj["isBitmapSent"].get<bool>();

  // bitmap_ is sent using arrow flight for performance
  // host_ and port_ are not needed in storage side

  return fpdbStoreFilterBitmapWrapper;
}

}
