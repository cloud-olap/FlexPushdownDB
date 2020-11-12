//
// Created by matt on 14/8/20.
//

#include "normal/pushdown/s3/S3SelectScan2.h"

#include <normal/pushdown/TupleMessage.h>
#include <normal/connector/s3/S3SelectPartition.h>
#include <normal/pushdown/cache/CacheHelper.h>

#include <utility>

using namespace normal::cache;
using namespace normal::core::cache;
using namespace normal::pushdown::cache;

using namespace normal::pushdown;

S3SelectScan2::S3SelectScan2(std::string name,
							 const std::string &s3Bucket,
							 const std::string &s3Object,
							 const std::string &sql,
							 std::optional<int64_t> startPos,
							 std::optional<int64_t> finishPos,
							 FileType fileType,
							 std::optional<std::vector<std::string>> columnNames,
							 const std::shared_ptr<arrow::Schema>& schema,
							 const std::optional<S3SelectCSVParseOptions> &csvParseOptions,
							 const std::shared_ptr<S3Client> &s3Client,
							 bool scanOnStart,
							 long queryId) :
	Operator(std::move(name), "S3SelectScan", queryId),
	columnNames_(std::move(columnNames)),
	scanOnStart_(scanOnStart),
	kernel_(S3SelectScanKernel::make(s3Bucket,
									 s3Object,
									 sql,
									 startPos,
									 finishPos,
									 fileType,
									 csvParseOptions,
									 s3Client,
									 schema)) {
}

/**
 *
 * @param name The name of the operator
 * @param s3Bucket The s3 bucket to read from
 * @param s3Object The s3 object to read from
 * @param sql The sql to run against the object
 * @param startPos The start of the s3 scan range (optional)
 * @param finishPos The end of the s3 scan range (optional)
 * @param fileType The file type (CSV or PARQUET)
 * @param columnNames The column names to read if scanOnStart is true (optional)
 * @param csvParseOptions The CSV parse options to use in the SelectObjectContent request
 * @param s3Client S3 client
 * @param scanOnStart To run the scan immediately on starting instead of waiting for a scan message
 * @return
 */
std::shared_ptr<S3SelectScan2> S3SelectScan2::make(const std::string &name,
												   const std::string &s3Bucket,
												   const std::string &s3Object,
												   const std::string &sql,
												   std::optional<int64_t> startPos,
												   std::optional<int64_t> finishPos,
												   FileType fileType,
												   const std::optional<std::vector<std::string>> &columnNames,
												   const std::shared_ptr<arrow::Schema>& schema,
												   const std::optional<S3SelectCSVParseOptions> &csvParseOptions,
												   const std::shared_ptr<S3Client> &s3Client,
												   bool scanOnStart,
												   long queryId) {
  return std::make_shared<S3SelectScan2>(name,
										 s3Bucket,
										 s3Object,
										 sql,
										 startPos,
										 finishPos,
										 fileType,
										 columnNames.has_value() ? std::optional(ColumnName::canonicalize(columnNames.value())) : std::nullopt,
										 schema,
										 csvParseOptions,
										 s3Client,
										 scanOnStart,
										 queryId);

}

tl::expected<void, std::string> S3SelectScan2::onStart() {
  SPDLOG_DEBUG("Starting operator  |  name: '{}'", this->name());
  if (scanOnStart_ && columnNames_.has_value()) {
	readAndSendTuples(columnNames_.value());
	ctx()->notifyComplete();
  }
  else if(scanOnStart_ && !columnNames_.has_value()){
    return tl::make_unexpected("scanOnStart is set but no column names were supplied");
  }
  else if(!scanOnStart_ && columnNamesToScan_.has_value()){
    // If we have already received column names, process them
	readAndSendTuples(columnNamesToScan_.value());
  }

  return {};
}

void S3SelectScan2::onReceive(const Envelope &message) {
  if (message.message().type() == "StartMessage") {
	auto result = this->onStart();
	if(!result.has_value())
	  throw std::runtime_error(result.error());
  } else if (message.message().type() == "ScanMessage") {
	auto scanMessage = dynamic_cast<const ScanMessage &>(message.message());
	this->onScan(scanMessage);
  } else if (message.message().type() == "CompleteMessage") {
	auto completeMessage = dynamic_cast<const CompleteMessage &>(message.message());
	onComplete(completeMessage);
  } else {
	// FIXME: Propagate error properly
	throw std::runtime_error("Unrecognized message type " + message.message().type());
  }
}

void S3SelectScan2::onComplete(const CompleteMessage &) {
  if (ctx()->operatorMap().allComplete(OperatorRelationshipType::Producer)) {
	ctx()->notifyComplete();
  }
}

void S3SelectScan2::onScan(const ScanMessage &Message) {
  columnNamesToScan_ = Message.getColumnNames();
  if(ctx()->operatorActor()->running_){
    // Only if running process the column names, otherwise leave for later
    readAndSendTuples(columnNamesToScan_.value());
  }
}

void S3SelectScan2::readAndSendTuples(const std::vector<std::string> &columnNames) {

  /*
   * FIXME: Should support reading the file in pieces
   */
  auto expectedReadTupleSet = kernel_->scan(columnNames);
  auto readTupleSet = expectedReadTupleSet.value();

  // Store the read columns in the cache
  requestStoreSegmentsInCache(readTupleSet);

  std::shared_ptr<Message> message = std::make_shared<TupleMessage>(readTupleSet->toTupleSetV1(), this->name());
  ctx()->tell(message);
}

void S3SelectScan2::requestStoreSegmentsInCache(const std::shared_ptr<TupleSet2> &tupleSet) {
  auto partition = std::make_shared<S3SelectPartition>(kernel_->getS3Bucket(), kernel_->getS3Object());
  CacheHelper::requestStoreSegmentsInCache(tupleSet,
										   partition,
										   kernel_->getStartPos().has_value() ?
										   kernel_->getStartPos().value() :
										   0,
										   kernel_->getFinishPos().has_value() ?
										   kernel_->getFinishPos().value() :
										   std::numeric_limits<long>::max(),
										   name(),
										   ctx());
}
