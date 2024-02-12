//
// Created by matt on 14/12/19.
//

#include <fpdb/executor/physical/s3/S3CSVParser.h>
#include <arrow/csv/api.h>
#include <arrow/csv/parser.h>
#include <arrow/io/api.h>
#include <spdlog/spdlog.h>
#include <utility>

namespace fpdb::executor::physical::s3 {

S3CSVParser::S3CSVParser(std::vector<std::string> columnNames,
							   std::shared_ptr<arrow::Schema> schema,
							   char csvDelimiter) :
	columnNames_(std::move(columnNames)),
	schema_(std::move(schema)),
	csvDelimiter_(csvDelimiter){}


std::shared_ptr<S3CSVParser> S3CSVParser::make(const std::vector<std::string>& columnNames,
                 const std::shared_ptr<arrow::Schema>& schema, char csvDelimiter) {
  return std::make_shared<S3CSVParser>(columnNames, schema, csvDelimiter);
}

/**
 *
 * @param from
 * @param to
 * @return
 */
tl::expected<std::shared_ptr<TupleSet>, std::string> S3CSVParser::parseCompletePayload(
	const Aws::Vector<unsigned char>::iterator &from,
	const Aws::Vector<unsigned char>::iterator &to) {

  //  Aws::String records(from, to);
  //  SPDLOG_DEBUG("records '{}'", records);

  auto parse_options = arrow::csv::ParseOptions::Defaults();
  parse_options.delimiter = csvDelimiter_;
  auto read_options = arrow::csv::ReadOptions::Defaults();
  read_options.use_threads = false;
  read_options.autogenerate_column_names = false;
  read_options.column_names = columnNames_;
  auto convert_options = arrow::csv::ConvertOptions::Defaults();
  std::unordered_map<std::string, std::shared_ptr<::arrow::DataType>> columnTypes;
  for(const auto &columnName: columnNames_){
	columnTypes.emplace(columnName, schema_->GetFieldByName(columnName)->type());
  }
  convert_options.column_types = columnTypes;

  // Create a reader
  ::arrow::io::IOContext ioContext;
  auto reader = std::make_shared<arrow::io::BufferReader>(from.base(), std::distance(from, to));
  auto createResult = arrow::io::BufferedInputStream::Create(CSV_READER_BUFFER_SIZE,
															 arrow::default_memory_pool(),
															 reader,
															 -1);
  if (!createResult.ok())
	return tl::make_unexpected(fmt::format("Cannot parse S3 Select payload  |  Could not create a reader, error: '{}'",
										 createResult.status().message()));
  auto input = *createResult;

  // Instantiate TableReader from input stream and options
  auto makeReaderResult = arrow::csv::TableReader::Make(ioContext,
														input,
														read_options,
														parse_options,
														convert_options);
  if (!makeReaderResult.ok())
	throw std::runtime_error(fmt::format(
		"Cannot parse S3 Select payload  |  Could not create a table reader, error: '{}'",
		makeReaderResult.status().message()));
  auto tableReader = *makeReaderResult;

  // Parse the payload and create the tupleset
  auto tupleSet = TupleSet::make(tableReader);

  return tupleSet;
}

/**
 *
 * @param payload
 * @return
 */
tl::expected<std::optional<std::shared_ptr<TupleSet>>,
			 std::string> S3CSVParser::parse(Aws::Vector<unsigned char> &payload) {

  SPDLOG_TRACE({
				 Aws::String payloadString(payload.begin(), payload.end());
				 spdlog::trace("Received payload. Payload: \n{}", payloadString);
			   });

  // Prepend previous partial data, if there is any

  SPDLOG_TRACE({
				 Aws::String currentPartialPayload(partial.begin(), partial.end());
				 spdlog::trace("Current partial payload: \n{}", currentPartialPayload);
			   });

  if (!partial.empty()) {
	payload.insert(payload.begin(), partial.begin(), partial.end());
	partial.clear();
  }

  SPDLOG_TRACE({
				 Aws::String prependedPartialPayload(partial.begin(), partial.end());
				 spdlog::trace("Prepended partial payload: \n{}", prependedPartialPayload);
			   });


  // Find the last newline (if there is one)
  auto newLineIt = std::find(payload.rbegin(), payload.rend(), '\n');
  long pos = (payload.rend() - newLineIt);

  // Store anything following the last newline (if there is one) as partial data
  partial = std::vector<unsigned char>(payload.begin() + pos, payload.end());

  SPDLOG_TRACE({
				 Aws::String newPartialPayload(partial.begin(), partial.end());
				 spdlog::trace("Partial payload: \n{}", newPartialPayload);
			   });

  SPDLOG_TRACE({
				 Aws::String completePayload(payload.begin(), payload.begin() + pos);
				 spdlog::trace("Complete payload: \n{}", records3);
			   });

  if (pos > 0) {
    auto expTupleSet = S3CSVParser::parseCompletePayload(payload.begin(), payload.begin() + pos);
    if (expTupleSet.has_value()) {
      return *expTupleSet;
    } else {
      return tl::make_unexpected(expTupleSet.error());
    }
  } else {
	  return std::nullopt;
  }
}

}
