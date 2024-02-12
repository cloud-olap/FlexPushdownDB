//
// Created by matt on 16/6/20.
//

#include <fpdb/executor/physical/s3/S3SelectCSVParseOptions.h>
#include <utility>

using namespace fpdb::executor::physical::s3;

S3SelectCSVParseOptions::S3SelectCSVParseOptions(std::string FieldDelimiter,
												 std::string RecordDelimiter)
	: fieldDelimiter_(std::move(FieldDelimiter)), recordDelimiter_(std::move(RecordDelimiter)) {}

const std::string &S3SelectCSVParseOptions::getFieldDelimiter() const {
  return fieldDelimiter_;
}

const std::string &S3SelectCSVParseOptions::getRecordDelimiter() const {
  return recordDelimiter_;
}
