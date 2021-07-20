//
// Created by matt on 14/8/20.
//

#include <normal/pushdown/s3/S3SelectScanKernel.h>
#include <normal/pushdown/s3/S3CSVParser.h>
#include <utility>
#include <normal/connector/MiniCatalogue.h>

using namespace Aws::Client;
using namespace Aws::S3;
using namespace Aws::S3::Model;
using namespace normal::pushdown::s3;

S3SelectScanKernel::S3SelectScanKernel(std::string s3Bucket,
									   std::string s3Object,
									   std::string sql,
									   std::optional<int64_t> startPos,
									   std::optional<int64_t> finishPos,
									   FileType fileType,
									   std::optional<S3SelectCSVParseOptions> csvParseOptions,
									   std::shared_ptr<Aws::S3::S3Client> s3Client,
									   std::shared_ptr<arrow::Schema> schema)
	: s3Bucket_(std::move(s3Bucket)),
	  s3Object_(std::move(s3Object)),
	  sql_(std::move(sql)),
	  startPos_(startPos),
	  finishPos_(finishPos),
	  fileType_(fileType),
	  csvParseOptions_(std::move(csvParseOptions)),
	  s3Client_(std::move(s3Client)),
	  schema_(std::move(schema)) {}

std::unique_ptr<S3SelectScanKernel> S3SelectScanKernel::make(const std::string &s3Bucket,
															 const std::string &s3Object,
															 const std::string &sql,
															 std::optional<int64_t> startPos,
															 std::optional<int64_t> finishPos,
															 FileType fileType,
															 const std::optional< S3SelectCSVParseOptions> &csvParseOptions,
															 const std::shared_ptr<Aws::S3::S3Client> &s3Client,
															 const std::shared_ptr<arrow::Schema>& schema) {
  return std::make_unique<S3SelectScanKernel>(s3Bucket,
											  s3Object,
											  sql,
											  startPos,
											  finishPos,
											  fileType,
											  csvParseOptions,
											  s3Client,
											  schema);

}

tl::expected<std::shared_ptr<TupleSet2>, std::string>
S3SelectScanKernel::scan(const std::vector<std::string> &columnNames) {

  std::optional<std::shared_ptr<TupleSet2>> tupleSet;
  std::optional<std::string> optionalError;

  if (columnNames.empty()) {
    // Makes an empty tuple set
	tupleSet = TupleSet2::make(std::vector<std::shared_ptr<Column>>{});
  } else {
	auto sql = fmt::format(sql_, fmt::join(columnNames, ","));

	auto result = s3Select(sql, columnNames, [&](const std::shared_ptr<TupleSet2> &tupleSetChunk) -> auto {

//	  tupleSetChunk->renameColumns(columnNames);

	  if (!tupleSet.has_value()) {
		tupleSet = tupleSetChunk;
	  } else {
		auto result = tupleSet.value()->append(tupleSetChunk);
		if (!result.has_value()) {
		  optionalError = result.error();
		  return;
		}
	  }
	});

	if (!result.has_value())
	  return tl::make_unexpected(result.error());
  }

  if (optionalError.has_value())
	return tl::make_unexpected(optionalError.value());

  return tupleSet.value();
}

tl::expected<void, std::string>
S3SelectScanKernel::s3Select(const std::string &sql,
							 const std::vector<std::string> &columnNames,
							 const S3SelectScanKernel::TupleSetEventCallback &tupleSetEventCallback) {

  std::optional<std::string> optionalErrorMessage;

  Aws::String bucketName = Aws::String(s3Bucket_);

  SelectObjectContentRequest selectObjectContentRequest;
  selectObjectContentRequest.SetBucket(bucketName);
  selectObjectContentRequest.SetKey(Aws::String(s3Object_));

  ScanRange scanRange;
  if (startPos_.has_value())
	scanRange.SetStart(startPos_.value());
  if (finishPos_.has_value())
	scanRange.SetEnd(finishPos_.value());
  selectObjectContentRequest.SetScanRange(scanRange);

  selectObjectContentRequest.SetExpressionType(ExpressionType::SQL);
  selectObjectContentRequest.SetExpression(sql.c_str());

  switch (fileType_) {
  case FileType::CSV: {
	CSVInput csvInput;
	csvInput.SetFileHeaderInfo(FileHeaderInfo::USE);

	if(csvParseOptions_.has_value()){
	  csvInput.SetFieldDelimiter(csvParseOptions_.value().getFieldDelimiter().c_str());
	  csvInput.SetRecordDelimiter(csvParseOptions_.value().getRecordDelimiter().c_str());
	}

	InputSerialization inputSerialization;
	inputSerialization.SetCSV(csvInput);
	selectObjectContentRequest.SetInputSerialization(inputSerialization);
	break;
  }
  case FileType::Parquet: {
	ParquetInput parquetInput;
	InputSerialization inputSerialization;
	inputSerialization.SetParquet(parquetInput);
	selectObjectContentRequest.SetInputSerialization(inputSerialization);
	break;
  }
  }

  CSVOutput csvOutput;
  OutputSerialization outputSerialization;
  outputSerialization.SetCSV(csvOutput);
  selectObjectContentRequest.SetOutputSerialization(outputSerialization);

  S3CSVParser s3CSVParser{columnNames, schema_, normal::connector::defaultMiniCatalogue->getCSVFileDelimiter()};

  SelectObjectContentHandler handler;
  handler.SetRecordsEventCallback([&](const RecordsEvent &recordsEvent) {
	SPDLOG_DEBUG("S3 Select RecordsEvent  |  partition: s3://{}/{}, size: {}",
				 s3Bucket_,
				 s3Object_,
				 recordsEvent.GetPayload().size());
	auto payload = recordsEvent.GetPayload();

	auto expectedTupleSet = s3CSVParser.parse(payload);

	// Check for error
	if (!expectedTupleSet.has_value()) {
	  optionalErrorMessage = expectedTupleSet.error();
	  return;
	}
	auto maybeTupleSet = expectedTupleSet.value();

	// Check if a tupleset was parsed
	if (maybeTupleSet.has_value()) {
	  auto tupleSet = TupleSet2::create(maybeTupleSet.value());
	  tupleSetEventCallback(tupleSet);
	}
  });
  handler.SetStatsEventCallback([&](const StatsEvent &statsEvent) {
	SPDLOG_DEBUG("S3 Select StatsEvent  |  partition: s3://{}/{}, scanned: {}, processed: {}, returned: {}",
				 s3Bucket_,
				 s3Object_,
				 statsEvent.GetDetails().GetBytesScanned(),
				 statsEvent.GetDetails().GetBytesProcessed(),
				 statsEvent.GetDetails().GetBytesReturned());
  });
  handler.SetEndEventCallback([&]() {
	SPDLOG_DEBUG("S3 Select EndEvent  |  partition: s3://{}/{}",
				 s3Bucket_,
				 s3Object_);
  });
  handler.SetOnErrorCallback([&](const AWSError<S3Errors> &errors) {
	SPDLOG_DEBUG("S3 Select Error  |  partition: s3://{}/{}, message: {}",
				 s3Bucket_,
				 s3Object_,
				 std::string(errors.GetMessage()));
	optionalErrorMessage = errors.GetMessage();
  });
  selectObjectContentRequest.SetEventStreamHandler(handler);

  auto selectObjectContentOutcome = this->s3Client_->SelectObjectContent(selectObjectContentRequest);

  if (!selectObjectContentOutcome.IsSuccess())
	return tl::make_unexpected(selectObjectContentOutcome.GetError().GetMessage().c_str());

  if (optionalErrorMessage.has_value())
	return tl::make_unexpected(optionalErrorMessage.value());

  return {};
}

const std::string &S3SelectScanKernel::getS3Bucket() const {
  return s3Bucket_;
}

const std::string &S3SelectScanKernel::getS3Object() const {
  return s3Object_;
}

std::optional<int64_t> S3SelectScanKernel::getStartPos() const {
  return startPos_;
}

std::optional<int64_t> S3SelectScanKernel::getFinishPos() const {
  return finishPos_;
}
