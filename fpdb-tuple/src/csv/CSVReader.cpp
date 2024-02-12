//
// Created by Yifei Yang on 2/18/22.
//

#include <fpdb/tuple/csv/CSVReader.h>
#include <fpdb/tuple/csv/CSVFormat.h>
#include <fpdb/tuple/csv/CSVParser.h>

#ifdef __AVX2__
#include <fpdb/tuple/arrow/CSVToArrowSIMDStreamParser.h>
#endif

namespace fpdb::tuple::csv {

tl::expected<std::shared_ptr<TupleSet>, std::string> CSVReader::read(const std::vector<std::string> &columnNames) {
#ifdef __AVX2__
  return readUsingSimdParser(columnNames);
#else
  return readUsingArrowApi(columnNames);
#endif
}

#ifdef __AVX2__
tl::expected<std::shared_ptr<TupleSet>, std::string>
CSVReader::readUsingSimdParserImpl(const std::vector<std::string> &columnNames,
                                   std::basic_istream<char, std::char_traits<char>> &inputStream) {
  // cast format
  std::shared_ptr<TupleSet> tupleSet;
  auto csvFormat = std::static_pointer_cast<CSVFormat>(format_);

  // make output schema
  ::arrow::FieldVector fields;
  for (const auto &columnName: columnNames) {
    auto field = schema_->GetFieldByName(columnName);
    if (!field) {
      return tl::make_unexpected(fmt::format("Read CSV Error: column {} not found", columnName));
    }
    fields.emplace_back(field);
  }
  auto outputSchema = ::arrow::schema(fields);

  // read and parse
  auto simdParser = CSVToArrowSIMDStreamParser(CSVToArrowSIMDStreamParser::DefaultParseChunkSize,
                                               inputStream,
                                               true,
                                               schema_,
                                               outputSchema,
                                               false,
                                               csvFormat->getFieldDelimiter());
  try {
    tupleSet = simdParser.constructTupleSet();
  } catch (const std::runtime_error &err) {
    return tl::make_unexpected(err.what());
  }

  return tupleSet;
}
#endif

tl::expected<std::shared_ptr<TupleSet>, std::string>
CSVReader::readUsingArrowApiImpl(const std::vector<std::string> &columnNames,
                                 const std::shared_ptr<::arrow::io::InputStream> &inputStream) {
  // cast format
  std::shared_ptr<TupleSet> tupleSet;
  auto csvFormat = std::static_pointer_cast<CSVFormat>(format_);

  // set options
  std::unordered_map<std::string, std::shared_ptr<::arrow::DataType>> columnTypes;
  for (const auto &field: schema_->fields()) {
    columnTypes.emplace(field->name(), field->type());
  }
  auto ioContext = arrow::io::IOContext();
  auto parse_options = arrow::csv::ParseOptions::Defaults();
  auto read_options = arrow::csv::ReadOptions::Defaults();
  auto convert_options = arrow::csv::ConvertOptions::Defaults();
  read_options.use_threads = false;
  read_options.skip_rows = csvFormat->getSkipRows(); // Skip the header
  read_options.column_names = schema_->field_names();
  parse_options.delimiter = csvFormat->getFieldDelimiter();
  convert_options.column_types = columnTypes;
  convert_options.include_columns = columnNames;

  // read input stream into table
  auto expTableReader = arrow::csv::TableReader::Make(ioContext,
                                                      inputStream,
                                                      read_options,
                                                      parse_options,
                                                      convert_options);
  if (!expTableReader.ok()) {
    return tl::make_unexpected(expTableReader.status().message());
  }

  return TupleSet::make(*expTableReader);
}

tl::expected<std::shared_ptr<TupleSet>, std::string>
CSVReader::readRangeImpl(const std::vector<std::string> &columnNames,
                         int64_t startPos,
                         int64_t finishPos,
                         const std::shared_ptr<::arrow::io::RandomAccessFile> &inputStream) {
  CSVParser parser(inputStream, schema_, columnNames, startPos, finishPos);
  return parser.parse();
}

}
