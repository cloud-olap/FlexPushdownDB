//
// Created by Yifei Yang on 2/13/22.
//

#include <fpdb/tuple/csv/CSVFormat.h>
#include <fmt/format.h>

using json = ::nlohmann::json;

namespace fpdb::tuple::csv {

CSVFormat::CSVFormat(char fieldDelimiter,
                     int skipRows) :
  FileFormat(FileFormatType::CSV),
  fieldDelimiter_(fieldDelimiter),
  skipRows_(skipRows) {}

char CSVFormat::getFieldDelimiter() const {
  return fieldDelimiter_;
}

int CSVFormat::getSkipRows() const {
  return skipRows_;
}

bool CSVFormat::isColumnar() const {
  return false;
}

json CSVFormat::toJson() const {
  json jObj;
  jObj.emplace("type", "CSV");
  jObj.emplace("fieldDelimiter", fieldDelimiter_);
  jObj.emplace("skipRows", skipRows_);
  return jObj;
}

tl::expected<std::shared_ptr<CSVFormat>, std::string> CSVFormat::fromJson(const nlohmann::json &jObj) {
  if (!jObj.contains("fieldDelimiter")) {
    return tl::make_unexpected(fmt::format("FieldDelimiter not specified in CSV format JSON '{}'", to_string(jObj)));
  }
  auto fieldDelimiter = jObj["fieldDelimiter"].get<char>();

  if (!jObj.contains("skipRows")) {
    return tl::make_unexpected(fmt::format("SkipRows not specified in CSV format JSON '{}'", to_string(jObj)));
  }
  auto skipRows = jObj["skipRows"].get<int>();

  return std::make_shared<CSVFormat>(fieldDelimiter, skipRows);
}

}
