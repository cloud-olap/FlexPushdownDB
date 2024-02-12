//
// Created by Yifei Yang on 2/13/22.
//

#include <fpdb/tuple/parquet/ParquetFormat.h>

using json = ::nlohmann::json;

namespace fpdb::tuple::parquet {

ParquetFormat::ParquetFormat() :
  FileFormat(FileFormatType::PARQUET) {}

bool ParquetFormat::isColumnar() const {
  return true;
}

json ParquetFormat::toJson() const {
  json jObj;
  jObj.emplace("type", "Parquet");
  return jObj;
}

tl::expected<std::shared_ptr<ParquetFormat>, std::string> ParquetFormat::fromJson(const nlohmann::json &) {
  return std::make_shared<ParquetFormat>();
}

}
