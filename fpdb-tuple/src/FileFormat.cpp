//
// Created by Yifei Yang on 2/13/22.
//

#include <fpdb/tuple/FileFormat.h>
#include <fpdb/tuple/csv/CSVFormat.h>
#include <fpdb/tuple/parquet/ParquetFormat.h>
#include <fmt/format.h>

namespace fpdb::tuple {

FileFormat::FileFormat(FileFormatType type) : type_(type) {}

FileFormatType FileFormat::getType() const {
  return type_;
}

tl::expected<std::shared_ptr<FileFormat>, std::string> FileFormat::fromJson(const nlohmann::json &jObj) {
  if (!jObj.contains("type")) {
    return tl::make_unexpected(fmt::format("Type not specified in file format JSON '{}'", to_string(jObj)));
  }
  auto type = jObj["type"].get<std::string>();

  if (type == "CSV") {
    return csv::CSVFormat::fromJson(jObj);
  } else if (type == "Parquet") {
    return parquet::ParquetFormat::fromJson(jObj);
  } else {
    return tl::make_unexpected(fmt::format("Unsupported file format type: '{}'", type));
  }
}

}
