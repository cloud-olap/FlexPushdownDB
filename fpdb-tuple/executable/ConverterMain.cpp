//
// Created by Yifei Yang on 3/8/22.
//

#include <fpdb/tuple/Converter.h>
#include <fpdb/tuple/csv/CSVFormat.h>
#include <fpdb/tuple/Globals.h>
#include <fpdb/tuple/ColumnName.h>
#include <fpdb/util/Util.h>
#include <tl/expected.hpp>
#include <nlohmann/json.hpp>

using namespace fpdb::tuple;

shared_ptr<arrow::DataType> strToDataType(const string &str) {
  if (str == "int32" || str == "int") {
    return arrow::int32();
  } else if (str == "int64" || str == "long") {
    return arrow::int64();
  } else if (str == "float64" || str == "double") {
    return arrow::float64();
  } else if (str == "utf8" || str == "string") {
    return arrow::utf8();
  } else if (str == "boolean" || str == "bool") {
    return arrow::boolean();
  } else if (str == "date") {
    return arrow::date64();
  } else {
    throw runtime_error(fmt::format("Unsupported data type: {}", str));
  }
}

tl::expected<void, std::string> readSchemaAndFormat(const std::string &schemaFormatFile,
                                                    std::shared_ptr<::arrow::Schema> &schema,
                                                    std::shared_ptr<csv::CSVFormat> &csvFormat) {
  auto jObj = nlohmann::json::parse(fpdb::util::readFile(schemaFormatFile));
  if (!jObj.is_object()) {
    return tl::make_unexpected(fmt::format("Error when parsing schema_format_file: {}", schemaFormatFile));
  }

  if (!jObj.contains("fields")) {
    return tl::make_unexpected(fmt::format("No field in schema_format_file: {}", schemaFormatFile));
  }
  auto fieldsJArr = jObj["fields"].get<std::vector<nlohmann::json>>();
  ::arrow::FieldVector fields;
  for (const auto &fieldJObj: fieldsJArr) {
    auto fieldName = ColumnName::canonicalize(fieldJObj["name"].get<string>());
    auto fieldTypeStr = fieldJObj["type"].get<string>();
    auto fieldType = strToDataType(fieldTypeStr);
    fields.emplace_back(std::make_shared<::arrow::Field>(fieldName, fieldType));
  }
  schema = ::arrow::schema(fields);

  if (!jObj.contains("format")) {
    return tl::make_unexpected(fmt::format("No format in schema_format_file: {}", schemaFormatFile));
  }
  auto formatJObj = jObj["format"];
  char fieldDelimiter = formatJObj["fieldDelimiter"].get<string>().c_str()[0];
  csvFormat = std::make_shared<csv::CSVFormat>(fieldDelimiter);

  return {};
}

int main(int argc, char *argv[]) {
  if (argc < 5) {
    fprintf(stderr, "Usage: %s <csv_file> <parquet_file> <compression_type> <schema_format_file> "
                    "(schema_json_file should be like the ones in "
                    "resources/scripts/convert_csv_to_parquet/tpch/schema_format)\n", argv[0]);
    return 1;
  }
  auto csvFile = std::string(argv[1]);
  auto parquetFile = std::string(argv[2]);
  auto compressionTypeStr = std::string(argv[3]);
  auto schemaFormatFile = std::string(argv[4]);

  ::parquet::Compression::type compressionType;
  if (compressionTypeStr == "uncompressed") {
    compressionType = ::parquet::Compression::type::UNCOMPRESSED;
  } else if (compressionTypeStr == "snappy") {
    compressionType = ::parquet::Compression::type::SNAPPY;
  } else {
    fprintf(stderr, "Currently only added snappy here, please add your compression type into the code");
    return 1;
  }

  std::shared_ptr<::arrow::Schema> schema;
  std::shared_ptr<csv::CSVFormat> csvFormat;
  auto result = readSchemaAndFormat(schemaFormatFile, schema, csvFormat);
  if (!result.has_value()) {
    fprintf(stderr, "Error when read schem_json_file: %s", result.error().c_str());
  }

  result = Converter::csvToParquet(csvFile, parquetFile, csvFormat, schema, DefaultChunkSize, compressionType);
  if (!result.has_value()) {
    fprintf(stderr, "Error when read converting CSV to Parquet: %s", result.error().c_str());
  }

  return 0;
}
